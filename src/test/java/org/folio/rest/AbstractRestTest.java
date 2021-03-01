package org.folio.rest;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.http.ContentType;
import io.restassured.specification.RequestSpecification;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import org.folio.rest.client.TenantClient;
import org.folio.rest.jaxrs.model.InitJobExecutionsRsDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionCollection;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.jaxrs.model.TenantJob;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.PomReader;
import org.folio.rest.tools.utils.NetworkUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.useDefaults;
import static org.folio.dataimport.util.RestUtil.OKAPI_TENANT_HEADER;
import static org.folio.dataimport.util.RestUtil.OKAPI_URL_HEADER;

/**
 * Abstract test for the REST API testing needs.
 */
public abstract class AbstractRestTest {

  private static final String FILE_EXTENSIONS_TABLE = "file_extensions";
  private static final String UPLOAD_DEFINITIONS_TABLE = "upload_definitions";
  private static final String HTTP_PORT = "http.port";
  protected static int port;
  private static String useExternalDatabase;
  private static Vertx vertx;
  protected static final String TENANT_ID = "diku";
  protected static final String TOKEN = "token";
  protected static RequestSpecification spec;
  protected static RequestSpecification specUpload;
  private static final String KAFKA_HOST = "KAFKA_HOST";
  private static final String KAFKA_PORT = "KAFKA_PORT";
  private static final String OKAPI_URL_ENV = "OKAPI_URL";
  private static final int PORT = NetworkUtils.nextFreePort();
  protected static final String OKAPI_URL = "http://localhost:" + PORT;

  private static final String GET_USER_URL = "/users?query=id==";

  private JsonObject userResponse = new JsonObject()
    .put("users",
      new JsonArray().add(new JsonObject()
        .put("username", "diku_admin")
        .put("personal", new JsonObject().put("firstName", "DIKU").put("lastName", "ADMINISTRATOR"))))
    .put("totalRecords", 1);

  private JobExecution jobExecution = new JobExecution()
    .withId(UUID.randomUUID().toString())
    .withHrId(1000)
    .withParentJobId(UUID.randomUUID().toString())
    .withSubordinationType(JobExecution.SubordinationType.PARENT_SINGLE)
    .withStatus(JobExecution.Status.NEW)
    .withUiStatus(JobExecution.UiStatus.INITIALIZATION)
    .withSourcePath("CornellFOLIOExemplars_Bibs.mrc")
    .withJobProfileInfo(new JobProfileInfo()
      .withName("Marc jobs profile")
      .withDataType(JobProfileInfo.DataType.MARC)
      .withId(UUID.randomUUID().toString()))
    .withUserId(UUID.randomUUID().toString());

  private JobExecutionCollection childrenJobExecutions = new JobExecutionCollection()
    .withJobExecutions(Arrays.asList(jobExecution.withId(UUID.randomUUID().toString()).withSubordinationType(JobExecution.SubordinationType.CHILD),
      jobExecution.withId(UUID.randomUUID().toString()).withSubordinationType(JobExecution.SubordinationType.CHILD)))
    .withTotalRecords(2);

  private JsonObject config = new JsonObject().put("totalRecords", 1)
    .put("configs", new JsonArray().add(new JsonObject()
      .put("module", "DATA_IMPORT")
      .put("code", "data.import.storage.path")
      .put("value", "./storage")
    ));

  private JsonObject config2 = new JsonObject().put("totalRecords", 1)
    .put("configs", new JsonArray().add(new JsonObject()
      .put("module", "DATA_IMPORT")
      .put("code", "data.import.storage.type")
      .put("value", "LOCAL_STORAGE")
    ));

  private InitJobExecutionsRsDto jobExecutionCreateSingleFile = new InitJobExecutionsRsDto()
    .withParentJobExecutionId(UUID.randomUUID().toString())
    .withJobExecutions(Collections.singletonList(
      new JobExecution().withId(UUID.randomUUID().toString()).withSourcePath("CornellFOLIOExemplars_Bibs(1).mrc")
    ));

  private InitJobExecutionsRsDto jobExecutionCreateMultipleFiles = new InitJobExecutionsRsDto()
    .withParentJobExecutionId(UUID.randomUUID().toString())
    .withJobExecutions(Arrays.asList(new JobExecution().withId(UUID.randomUUID().toString()).withSourcePath("CornellFOLIOExemplars_Bibs(1).mrc"),
      new JobExecution().withId(UUID.randomUUID().toString()).withSourcePath("CornellFOLIOExemplars.mrc")));

  @ClassRule
  public static EmbeddedKafkaCluster cluster = provisionWith(useDefaults());

  @Rule
  public WireMockRule mockServer = new WireMockRule(
    WireMockConfiguration.wireMockConfig()
      .dynamicPort()
      .notifier(new Slf4jNotifier(true)));

  @BeforeClass
  public static void setUpClass(final TestContext context) throws Exception {
    Async async = context.async();
    vertx = Vertx.vertx();
    String[] hostAndPort = cluster.getBrokerList().split(":");

    System.setProperty(KAFKA_HOST, hostAndPort[0]);
    System.setProperty(KAFKA_PORT, hostAndPort[1]);
    System.setProperty(OKAPI_URL_ENV, OKAPI_URL);

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
        TenantAttributes tenantAttributes = new TenantAttributes();
        tenantAttributes.setModuleTo(PomReader.INSTANCE.getModuleName());
        tenantClient.postTenant(tenantAttributes, res2 -> {
          if (res2.result().statusCode() == 204) {
            return;
          } if (res2.result().statusCode() == 201) {
            tenantClient.getTenantByOperationId(res2.result().bodyAsJson(TenantJob.class).getId(), 60000, context.asyncAssertSuccess(res3 -> {
              context.assertTrue(res3.bodyAsJson(TenantJob.class).getComplete());
              String error = res3.bodyAsJson(TenantJob.class).getError();
              if (error != null) {
                context.assertEquals("Failed to make post tenant. Received status code 400", error);
              }
            }));
          } else {
            context.assertEquals("Failed to make post tenant. Received status code 400", res2.result().bodyAsString());
          }
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
      cluster.close();
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
      WireMock.stubFor(WireMock.get(GET_USER_URL + okapiUserIdHeader)
        .willReturn(WireMock.okJson(userResponse.toString())));
      WireMock.stubFor(WireMock.get("/configurations/entries?query="
        + URLEncoder.encode("module==DATA_IMPORT AND ( code==\"data.import.storage.path\")", StandardCharsets.UTF_8)
        + "&offset=0&limit=3&")
        .willReturn(WireMock.okJson(config.toString())));
      WireMock.stubFor(WireMock.get("/configurations/entries?query="
        + URLEncoder.encode("module==DATA_IMPORT AND ( code==\"data.import.storage.type\")", StandardCharsets.UTF_8)
        + "&offset=0&limit=3&")
        .willReturn(WireMock.okJson(config2.toString())));
      WireMock.stubFor(WireMock.post("/change-manager/jobExecutions").withRequestBody(matchingJsonPath("$[?(@.files.size() == 1)]"))
        .willReturn(WireMock.created().withBody(JsonObject.mapFrom(jobExecutionCreateSingleFile).toString())));
      WireMock.stubFor(WireMock.post("/change-manager/jobExecutions").withRequestBody(matchingJsonPath("$[?(@.files.size() == 2)]"))
        .willReturn(WireMock.created().withBody(JsonObject.mapFrom(jobExecutionCreateMultipleFiles).toString())));
      WireMock.stubFor(WireMock.put(new UrlPathPattern(new RegexPattern("/change-manager/jobExecutions/.*"), true))
        .willReturn(WireMock.ok()));
      WireMock.stubFor(WireMock.get(new UrlPathPattern(new RegexPattern("/change-manager/jobExecutions/.{36}"), true))
        .willReturn(WireMock.ok().withBody(JsonObject.mapFrom(jobExecution).toString())));
      WireMock.stubFor(WireMock.get(new UrlPathPattern(new RegexPattern("/change-manager/jobExecutions/.{36}/children"), true))
        .willReturn(WireMock.ok().withBody(JsonObject.mapFrom(childrenJobExecutions).toString())));
  }

  private void clearTable(TestContext context) {
    Async async = context.async();
    PostgresClient.getInstance(vertx, TENANT_ID).delete(FILE_EXTENSIONS_TABLE, new Criterion(), event1 -> {
      PostgresClient.getInstance(vertx, TENANT_ID).delete(UPLOAD_DEFINITIONS_TABLE, new Criterion(), event2 -> {
        if (event1.failed()) {
          context.fail(event1.cause());
        }
        async.complete();
      });
    });
  }

}
