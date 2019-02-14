package org.folio.rest;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import com.jayway.restassured.builder.RequestSpecBuilder;
import com.jayway.restassured.http.ContentType;
import com.jayway.restassured.specification.RequestSpecification;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import org.folio.rest.client.TenantClient;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.NetworkUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static org.folio.dataimport.util.RestUtil.OKAPI_TENANT_HEADER;
import static org.folio.dataimport.util.RestUtil.OKAPI_URL_HEADER;

/**
 * Abstract test for the REST API testing needs.
 */
public abstract class AbstractRestTest {

  private static final String JOB_EXECUTIONS_TABLE_NAME = "job_executions";
  private static final String FILE_EXTENSIONS_TABLE = "file_extensions";
  private static final String UPLOAD_DEFINITIONS_TABLE = "upload_definitions";
  private static final String TOKEN = "token";
  private static final String HTTP_PORT = "http.port";
  private static int port;
  private static String useExternalDatabase;
  private static Vertx vertx;
  private static final String TENANT_ID = "diku";
  protected static RequestSpecification spec;
  protected static RequestSpecification specUpload;

  private static final String GET_USER_URL = "/users?query=id==";

  private JsonObject userResponse = new JsonObject()
    .put("users",
      new JsonArray().add(new JsonObject()
        .put("username", "diku_admin")
        .put("personal", new JsonObject().put("firstName", "DIKU").put("lastName", "ADMINISTRATOR"))))
    .put("totalRecords", 1);

  private JsonObject jobExecution = new JsonObject()
    .put("id", "5105b55a-b9a3-4f76-9402-a5243ea63c95")
    .put("hrId", "1000")
    .put("parentJobId", "5105b55a-b9a3-4f76-9402-a5243ea63c95")
    .put("subordinationType", "PARENT_SINGLE")
    .put("status", "NEW")
    .put("uiStatus", "INITIALIZATION")
    .put("sourcePath", "CornellFOLIOExemplars_Bibs.mrc")
    .put("jobProfileName", "Marc jobs profile")
    .put("userId", UUID.randomUUID().toString());

  private JsonObject childrenJobExecutions = new JsonObject()
    .put("jobExecutions", new JsonArray()
      .add(jobExecution).add(jobExecution))
    .put("totalRecords", 2);

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

  private static JsonObject jobExecutionCreateSingleFile = new JsonObject()
    .put("parentJobExecutionId", UUID.randomUUID().toString())
    .put("jobExecutions", new JsonArray()
      .add(new JsonObject()
        .put("sourcePath", "CornellFOLIOExemplars_Bibs.mrc")
        .put("id", UUID.randomUUID().toString())
      ));

  private static JsonObject jobExecutionCreateMultipleFiles = new JsonObject()
    .put("parentJobExecutionId", UUID.randomUUID().toString())
    .put("jobExecutions", new JsonArray()
      .add(new JsonObject()
        .put("sourcePath", "CornellFOLIOExemplars_Bibs.mrc")
        .put("id", UUID.randomUUID().toString()))
      .add(new JsonObject()
        .put("sourcePath", "CornellFOLIOExemplars.mrc")
        .put("id", UUID.randomUUID().toString())));

  @Rule
  public WireMockRule mockServer = new WireMockRule(
    WireMockConfiguration.wireMockConfig()
      .dynamicPort()
      .notifier(new Slf4jNotifier(true)));

  @BeforeClass
  public static void setUpClass(final TestContext context) throws Exception {
    Async async = context.async();
    vertx = Vertx.vertx();
    port = NetworkUtils.nextFreePort();
    String okapiUrl = "http://localhost:" + port;
    PostgresClient.stopEmbeddedPostgres();
    PostgresClient.closeAllClients();
    useExternalDatabase = System.getProperty(
      "org.folio.source.record.manager.test.database",
      "embedded");

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

    TenantClient tenantClient = new TenantClient(okapiUrl, TENANT_ID, TOKEN);

    final DeploymentOptions options = new DeploymentOptions().setConfig(new JsonObject().put(HTTP_PORT, port));
    vertx.deployVerticle(RestVerticle.class.getName(), options, res -> {
      try {
        TenantAttributes tenantAttributes = null;
        tenantClient.postTenant(tenantAttributes, res2 -> {
          async.complete();
        });
      } catch (Exception e) {
        e.printStackTrace();
      }
    });
  }

  @AfterClass
  public static void tearDownClass(final TestContext context) {
    Async async = context.async();
    vertx.close(context.asyncAssertSuccess(res -> {
      if (useExternalDatabase.equals("embedded")) {
        PostgresClient.stopEmbeddedPostgres();
      }
      async.complete();
    }));
  }

  @Before
  public void setUp(TestContext context) {
    clearTable(context);
    String okapiUserIdHeader = UUID.randomUUID().toString();
    spec = new RequestSpecBuilder()
      .setContentType(ContentType.JSON)
      .addHeader(OKAPI_URL_HEADER, "http://localhost:" + mockServer.port())
      .addHeader(OKAPI_TENANT_HEADER, TENANT_ID)
      .addHeader(RestVerticle.OKAPI_USERID_HEADER, okapiUserIdHeader)
      .addHeader("Accept", "text/plain, application/json")
      .setBaseUri("http://localhost:" + port)
      .build();
    specUpload = new RequestSpecBuilder()
      .setContentType("application/octet-stream")
      .addHeader(OKAPI_URL_HEADER, "http://localhost:" + mockServer.port())
      .addHeader(OKAPI_TENANT_HEADER, TENANT_ID)
      .addHeader(RestVerticle.OKAPI_USERID_HEADER, UUID.randomUUID().toString())
      .setBaseUri("http://localhost:" + port)
      .addHeader("Accept", "text/plain, application/json")
      .build();
    Map<String, String> okapiHeaders = new HashMap<>();
    okapiHeaders.put(OKAPI_URL_HEADER, "http://localhost:" + mockServer.port());
    okapiHeaders.put(OKAPI_TENANT_HEADER, TENANT_ID);
    okapiHeaders.put(RestVerticle.OKAPI_HEADER_TOKEN, TOKEN);
    okapiHeaders.put(RestVerticle.OKAPI_USERID_HEADER, okapiUserIdHeader);

    try {
      WireMock.stubFor(WireMock.get(GET_USER_URL + okapiUserIdHeader)
        .willReturn(WireMock.okJson(userResponse.toString())));
      WireMock.stubFor(WireMock.get("/configurations/entries?query="
        + URLEncoder.encode("module==DATA_IMPORT AND ( code==\"data.import.storage.path\")", "UTF-8")
        + "&offset=0&limit=3&")
        .willReturn(WireMock.okJson(config.toString())));
      WireMock.stubFor(WireMock.get("/configurations/entries?query="
        + URLEncoder.encode("module==DATA_IMPORT AND ( code==\"data.import.storage.type\")", "UTF-8")
        + "&offset=0&limit=3&")
        .willReturn(WireMock.okJson(config2.toString())));
      WireMock.stubFor(WireMock.post("/change-manager/jobExecutions").withRequestBody(matchingJsonPath("$[?(@.files.size() == 1)]"))
        .willReturn(WireMock.created().withBody(jobExecutionCreateSingleFile.toString())));
      WireMock.stubFor(WireMock.post("/change-manager/jobExecutions").withRequestBody(matchingJsonPath("$[?(@.files.size() == 2)]"))
        .willReturn(WireMock.created().withBody(jobExecutionCreateMultipleFiles.toString())));
      WireMock.stubFor(WireMock.put(new UrlPathPattern(new RegexPattern("/change-manager/jobExecution/.*"), true))
        .willReturn(WireMock.ok()));
      WireMock.stubFor(WireMock.get(new UrlPathPattern(new RegexPattern("/change-manager/jobExecution/.*{36}"), true))
        .willReturn(WireMock.ok().withBody(jobExecution.toString())));
      WireMock.stubFor(WireMock.get(new UrlPathPattern(new RegexPattern("/change-manager/jobExecution/.*{36}/children"), true))
        .willReturn(WireMock.ok().withBody(childrenJobExecutions.toString())));

    } catch (UnsupportedEncodingException ignored) {
    }

  }

  private void clearTable(TestContext context) {
    PostgresClient.getInstance(vertx, TENANT_ID).delete(JOB_EXECUTIONS_TABLE_NAME, new Criterion(), event1 -> {
      PostgresClient.getInstance(vertx, TENANT_ID).delete(FILE_EXTENSIONS_TABLE, new Criterion(), event2 -> {
        PostgresClient.getInstance(vertx, TENANT_ID).delete(UPLOAD_DEFINITIONS_TABLE, new Criterion(), event3 -> {
          if (event2.failed()) {
            context.fail(event2.cause());
          }
        });
      });
    });
  }

}
