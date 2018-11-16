package org.folio.rest;


import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.ConsoleNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.builder.RequestSpecBuilder;
import com.jayway.restassured.http.ContentType;
import com.jayway.restassured.specification.RequestSpecification;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.folio.rest.client.TenantClient;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.NetworkUtils;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Objects;
import java.util.UUID;

import static org.folio.util.ConfigurationUtil.OKAPI_TENANT_HEADER;
import static org.folio.util.ConfigurationUtil.OKAPI_TOKEN_HEADER;
import static org.folio.util.ConfigurationUtil.OKAPI_URL_HEADER;

@RunWith(VertxUnitRunner.class)
public class RestVerticleTest {

  private static final String TENANT = "diku";
  private static final String DEFINITION_PATH = "/data-import/upload/definition";
  private static final String FILE_PATH = "/data-import/upload/file";
  private static final String FILE_DEF_PATH = "/data-import/upload/definition/file";
  private static final String UPLOAD_DEFINITION_TABLE = "uploadDefinition";

  private static Vertx vertx;
  private static RequestSpecification spec;
  private static RequestSpecification specUpload;
  private static int port;

  private static JsonObject file1 = new JsonObject()
    .put("name", "bib.mrc");
  private static JsonObject file2 = new JsonObject()
    .put("name", "host.mrc");

  private static JsonObject uploadDef1 = new JsonObject()
    .put("fileDefinitions", new JsonArray().add(file1).add(file2));

  private static JsonObject uploadDef2 = new JsonObject()
    .put("fileDefinitions", new JsonArray().add(file1));

  private static JsonObject uploadDef3 = new JsonObject()
    .put("fileDefinitions", new JsonArray().add(file1));

  private static JsonObject config = new JsonObject().put("totalRecords", 1)
    .put("configs", new JsonArray().add(new JsonObject()
      .put("module", "DATA_IMPORT")
      .put("code", "data.import.storage.path")
      .put("value", "./storage")
    ));

  private static JsonObject config2 = new JsonObject().put("totalRecords", 1)
    .put("configs", new JsonArray().add(new JsonObject()
      .put("module", "DATA_IMPORT")
      .put("code", "data.import.storage.type")
      .put("value", "LOCAL_STORAGE")
    ));

  private void clearTable(TestContext context) {
    PostgresClient.getInstance(vertx, TENANT).delete(UPLOAD_DEFINITION_TABLE, new Criterion(), event -> {
      if (event.failed()) {
        context.fail(event.cause());
      }
    });
  }

  @Rule
  public WireMockRule userMockServer = new WireMockRule(
    WireMockConfiguration.wireMockConfig()
      .dynamicPort()
      .notifier(new ConsoleNotifier(true)));

  @BeforeClass
  public static void setUpClass(final TestContext context) throws Exception {
    Async async = context.async();
    PostgresClient.stopEmbeddedPostgres();
    vertx = Vertx.vertx();
    port = NetworkUtils.nextFreePort();

    String useExternalDatabase = System.getProperty(
      "org.folio.data.import.test.database",
      "embedded");

    switch (useExternalDatabase) {
      case "environment":
        System.out.println("Using environment settings");
        break;
      case "external":
        String postgresConfigPath = System.getProperty(
          "org.folio.data.import.test.config",
          "/postgres-conf-local.json");
        PostgresClient.setConfigFilePath(postgresConfigPath);
        break;
      case "embedded":
        PostgresClient.setIsEmbedded(true);
        PostgresClient.getInstance(vertx).startEmbeddedPostgres();
        break;
      default:
        String message = "No understood database choice made." +
          "Please set org.folio.data.import.test.database" +
          "to 'external', 'environment' or 'embedded'";
        throw new Exception(message);
    }

    TenantClient tenantClient = new TenantClient("localhost", port, "diku", "dummy-token");
    DeploymentOptions restVerticleDeploymentOptions = new DeploymentOptions()
      .setConfig(new JsonObject().put("http.port", port));
    vertx.deployVerticle(RestVerticle.class.getName(), restVerticleDeploymentOptions, res -> {
      try {
        tenantClient.postTenant(null, res2 ->
          async.complete()
        );
      } catch (Exception e) {
        e.printStackTrace();
      }
    });
  }

  @Before
  public void setUp(TestContext context) {
    spec = new RequestSpecBuilder()
      .setContentType(ContentType.JSON)
      .addHeader(OKAPI_URL_HEADER, "http://localhost:" + userMockServer.port())
      .addHeader(OKAPI_TENANT_HEADER, TENANT)
      .setBaseUri("http://localhost:" + port)
      .addHeader(RestVerticle.OKAPI_HEADER_TENANT, TENANT)
      .build();

    specUpload = new RequestSpecBuilder()
      .setContentType("application/octet-stream")
      .addHeader(OKAPI_URL_HEADER, "http://localhost:" + userMockServer.port())
      .addHeader(OKAPI_TENANT_HEADER, TENANT)
      .addHeader(OKAPI_TOKEN_HEADER, "dummy")
      .setBaseUri("http://localhost:" + port)
      .addHeader("Accept", "text/plain, application/json")
      .addHeader(RestVerticle.OKAPI_HEADER_TENANT, TENANT)
      .build();
    clearTable(context);
    try {
      WireMock.stubFor(WireMock.get("/configurations/entries?query="
        + URLEncoder.encode("module==DATA_IMPORT AND ( code==\"data.import.storage.path\")", "UTF-8")
        + "&offset=0&limit=3&")
        .willReturn(WireMock.okJson(config.toString())));
      WireMock.stubFor(WireMock.get("/configurations/entries?query="
        + URLEncoder.encode("module==DATA_IMPORT AND ( code==\"data.import.storage.type\")", "UTF-8")
        + "&offset=0&limit=3&")
        .willReturn(WireMock.okJson(config2.toString())));
    } catch (UnsupportedEncodingException ignored) {
    }
  }

  @Test
  public void uploadDefinitionCreate() {
    RestAssured.given()
      .spec(spec)
      .body(uploadDef1.encode())
      .when()
      .post(DEFINITION_PATH)
      .then()
      .log().all()
      .statusCode(HttpStatus.SC_CREATED)
      .body("metaJobExecutionId", Matchers.notNullValue())
      .body("id", Matchers.notNullValue())
      .body("status", Matchers.is("NEW"));
  }

  @Test
  public void uploadDefinitionGet() {
    String id = RestAssured.given()
      .spec(spec)
      .body(uploadDef1.encode())
      .when()
      .post(DEFINITION_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .log().all().extract().body().jsonPath().get("id");
    RestAssured.given()
      .spec(spec)
      .when()
      .get(DEFINITION_PATH + "?query=id==" + id)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", Matchers.is(1))
      .log().all();
  }

  @Test
  public void uploadDefinitionGetNotFound() {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(DEFINITION_PATH + "?query=id==" + UUID.randomUUID().toString())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", Matchers.is(0))
      .log().all();
  }

  @Test
  public void uploadDefinitionGetById() {
    String id = RestAssured.given()
      .spec(spec)
      .body(uploadDef2.encode())
      .when()
      .post(DEFINITION_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .log().all().extract().body().jsonPath().get("id");
    RestAssured.given()
      .spec(spec)
      .when()
      .get(DEFINITION_PATH + "/" + id)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("metaJobExecutionId", Matchers.notNullValue())
      .body("id", Matchers.notNullValue())
      .body("status", Matchers.is("NEW"))
      .log().all();
  }

  @Test
  public void uploadDefinitionGetByIdNotFound() {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(DEFINITION_PATH + "/" + UUID.randomUUID())
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND)
      .log().all();
  }

  @Test
  public void uploadDefinitionUpdate() {
    String object = RestAssured.given()
      .spec(spec)
      .body(uploadDef3.encode())
      .when()
      .post(DEFINITION_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .log().all()
      .extract().body().jsonPath().prettify();
    JsonObject jsonObject = new JsonObject(object);
    jsonObject.put("status", "LOADED");
    RestAssured.given()
      .spec(spec)
      .body(jsonObject.encode())
      .when()
      .put(DEFINITION_PATH + "/" + jsonObject.getString("id"))
      .then()
      .statusCode(HttpStatus.SC_OK)
      .log().all()
      .body("status", Matchers.is("LOADED"));
  }

  @Test
  public void uploadDefinitionUpdateNotFound() {
    RestAssured.given()
      .spec(spec)
      .body(uploadDef3.encode())
      .when()
      .put(DEFINITION_PATH + "/" + UUID.randomUUID().toString())
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND)
      .log().all();
  }

  @Test
  public void fileUpload() {
    String object = RestAssured.given()
      .spec(spec)
      .body(uploadDef3.encode())
      .when()
      .post(DEFINITION_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .log().all()
      .extract().body().jsonPath().prettify();
    JsonObject jsonObject = new JsonObject(object);
    String uploadDefId = jsonObject.getString("id");
    String fileId = jsonObject
      .getJsonArray("fileDefinitions")
      .getJsonObject(0)
      .getString("id");

    ClassLoader classLoader = getClass().getClassLoader();
    File file = new File(Objects.requireNonNull(classLoader.getResource("CornellFOLIOExemplars_Bibs.mrc")).getFile());
    RestAssured.given()
      .spec(specUpload)
      .when()
      .body(file)
      .post(FILE_PATH + "?uploadDefinitionId=" + uploadDefId + "&fileId=" + fileId)
      .then()
      .log().all()
      .statusCode(HttpStatus.SC_OK)
      .body("status", Matchers.is("IN_PROGRESS"));
  }

  @Test
  public void fileUploadNotFound() {
    ClassLoader classLoader = getClass().getClassLoader();
    File file = new File(Objects.requireNonNull(classLoader.getResource("CornellFOLIOExemplars_Bibs.mrc")).getFile());
    RestAssured.given()
      .spec(specUpload)
      .when()
      .body(file)
      .post(FILE_PATH
        + "?uploadDefinitionId=" + UUID.randomUUID().toString()
        + "&fileId=" + UUID.randomUUID().toString())
      .then()
      .log().all()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void fileDelete() {
    String object = RestAssured.given()
      .spec(spec)
      .body(uploadDef3.encode())
      .when()
      .post(DEFINITION_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .log().all()
      .extract().body().jsonPath().prettify();
    JsonObject jsonObject = new JsonObject(object);
    RestAssured.given()
      .spec(spec)
      .when()
      .delete(FILE_DEF_PATH + "/"
        + jsonObject.getJsonArray("fileDefinitions")
        .getJsonObject(0)
        .getString("id")
        + "?uploadDefinitionId=" + jsonObject.getString("id")
      )
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT)
      .log().all();
  }

  @Test
  public void fileDeleteNotFound() {
    RestAssured.given()
      .spec(spec)
      .when()
      .delete(FILE_DEF_PATH + "/"
        + UUID.randomUUID().toString()
        + "?uploadDefinitionId=" + UUID.randomUUID().toString()
      )
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND)
      .log().all();
  }
}
