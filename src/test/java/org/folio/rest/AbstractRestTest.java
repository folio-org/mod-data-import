package org.folio.rest;

import static com.github.tomakehurst.wiremock.client.WireMock.created;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.put;

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
import lombok.extern.log4j.Log4j2;
import org.folio.liquibase.LiquibaseUtil;
import org.folio.postgres.testing.PostgresTesterContainer;
import org.folio.rest.client.TenantClient;
import org.folio.rest.jaxrs.model.InitJobExecutionsRsDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionDto;
import org.folio.rest.jaxrs.model.JobExecutionDtoCollection;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.jaxrs.model.TenantJob;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.ModuleName;
import org.folio.rest.tools.utils.NetworkUtils;
import org.folio.s3.client.FolioS3Client;
import org.folio.s3.client.S3ClientFactory;
import org.folio.s3.client.S3ClientProperties;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static org.folio.dataimport.util.RestUtil.OKAPI_TENANT_HEADER;
import static org.folio.dataimport.util.RestUtil.OKAPI_URL_HEADER;

/**
 * Abstract test for the REST API testing needs.
 */
@Log4j2
@Testcontainers
public abstract class AbstractRestTest {

  private static final String FILE_EXTENSIONS_TABLE = "file_extensions";
  private static final String UPLOAD_DEFINITIONS_TABLE = "upload_definitions";
  private static final String HTTP_PORT = "http.port";
  public static final String TEST_MODULE_VERSION = "-1.0.0";
  protected static int port;
  private static String useExternalDatabase;
  protected static Vertx vertx;
  protected static final String TENANT_ID = "diku";
  protected static final String TOKEN = "token";
  protected static RequestSpecification spec;
  protected static RequestSpecification specUpload;
  private static final String KAFKA_HOST = "KAFKA_HOST";
  private static final String KAFKA_PORT = "KAFKA_PORT";
  private static final String KAFKA_MAX_REQUEST_SIZE = "MAX_REQUEST_SIZE";
  private static final String OKAPI_URL_ENV = "OKAPI_URL";
  private static final int PORT = NetworkUtils.nextFreePort();
  protected static final String OKAPI_URL = "http://localhost:" + PORT;
  protected static final String MINIO_BUCKET = "test-bucket";

  private static final String GET_USER_URL = "/users?query=id==";

  private JsonObject userResponse = new JsonObject()
    .put("users",
      new JsonArray().add(new JsonObject()
        .put("username", "diku_admin")
        .put("personal", new JsonObject().put("firstName", "DIKU").put("lastName", "ADMINISTRATOR"))))
    .put("totalRecords", 1);

  private JobExecutionDto jobExecution = new JobExecutionDto()
    .withId(UUID.randomUUID().toString())
    .withHrId(1000)
    .withParentJobId(UUID.randomUUID().toString())
    .withSubordinationType(JobExecutionDto.SubordinationType.PARENT_SINGLE)
    .withStatus(JobExecutionDto.Status.NEW)
    .withUiStatus(JobExecutionDto.UiStatus.INITIALIZATION)
    .withSourcePath("CornellFOLIOExemplars_Bibs.mrc")
    .withJobProfileInfo(new JobProfileInfo()
      .withName("Marc jobs profile")
      .withDataType(JobProfileInfo.DataType.MARC)
      .withId(UUID.randomUUID().toString()))
    .withUserId(UUID.randomUUID().toString());

  private JobExecutionDtoCollection childrenJobExecutions = new JobExecutionDtoCollection()
    .withJobExecutions(Arrays.asList(jobExecution.withId(UUID.randomUUID().toString()).withSubordinationType(JobExecutionDto.SubordinationType.CHILD),
      jobExecution.withId(UUID.randomUUID().toString()).withSubordinationType(JobExecutionDto.SubordinationType.CHILD)))
    .withTotalRecords(2);

  private InitJobExecutionsRsDto jobExecutionCreateSingleFile = new InitJobExecutionsRsDto()
    .withParentJobExecutionId(UUID.randomUUID().toString())
    .withJobExecutions(Collections.singletonList(
      new JobExecution().withId(UUID.randomUUID().toString()).withSourcePath("CornellFOLIOExemplars_Bibs(1).mrc")
    ));

  private InitJobExecutionsRsDto jobExecutionCreateMultipleFiles = new InitJobExecutionsRsDto()
    .withParentJobExecutionId(UUID.randomUUID().toString())
    .withJobExecutions(Arrays.asList(new JobExecution().withId(UUID.randomUUID().toString()).withSourcePath("CornellFOLIOExemplars_Bibs(1).mrc"),
      new JobExecution().withId(UUID.randomUUID().toString()).withSourcePath("CornellFOLIOExemplars.mrc")));

  @Container
  public static final KafkaContainer kafkaContainer = new KafkaContainer("apache/kafka-native:3.8.0")
      .withStartupAttempts(3);

  @Container
  private static final LocalStackContainer localStackContainer = new LocalStackContainer(
    DockerImageName.parse("localstack/localstack:s3-latest")
  )
    .withServices(LocalStackContainer.Service.S3);

  protected static FolioS3Client s3Client;

  @ClassRule
  public static WireMockRule mockServer = new WireMockRule(
    WireMockConfiguration.wireMockConfig()
      .dynamicPort()
      .notifier(new Slf4jNotifier(true)), true);

  @BeforeClass
  public static void setUpClass(final TestContext context) throws Exception {
    Async async = context.async();
    vertx = Vertx.vertx();

    log.info("Starting Kafka...");
    kafkaContainer.start();

    System.setProperty(KAFKA_HOST, kafkaContainer.getHost());
    System.setProperty(KAFKA_PORT, kafkaContainer.getFirstMappedPort() + "");
    System.setProperty(KAFKA_MAX_REQUEST_SIZE, "1048576");
    System.setProperty(OKAPI_URL_ENV, OKAPI_URL);

    log.info("Starting LocalStack/S3...");
    localStackContainer.start();
    log.info("Started LocalStack/S3 at {}", localStackContainer.getEndpoint().toString());
    System.setProperty("minio.endpoint", localStackContainer.getEndpoint().toString());
    System.setProperty("minio.region", localStackContainer.getRegion());
    System.setProperty("minio.accessKey", localStackContainer.getAccessKey());
    System.setProperty("minio.secretKey", localStackContainer.getSecretKey());
    System.setProperty("minio.bucket", MINIO_BUCKET);
    System.setProperty("minio.awsSdk", "false");

    s3Client = S3ClientFactory.getS3Client(
      S3ClientProperties
        .builder()
        .endpoint(localStackContainer.getEndpoint().toString())
        .accessKey(localStackContainer.getAccessKey())
        .secretKey(localStackContainer.getSecretKey())
        .bucket(MINIO_BUCKET)
        .awsSdk(false)
        .region(localStackContainer.getRegion())
        .build()
    );
    s3Client.createBucketIfNotExists();

    port = NetworkUtils.nextFreePort();
    String okapiUrl = "http://localhost:" + port;
    PostgresClient.stopPostgresTester();
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
        PostgresClient.setPostgresTester(new PostgresTesterContainer());
        LiquibaseUtil.initializeSchemaForModule(vertx, "data_import_global");
        break;
      default:
        String message = "No understood database choice made." +
          "Please set org.folio.data.import.test.database" +
          "to 'external', 'environment' or 'embedded'";
        throw new Exception(message);
    }

    TenantClient tenantClient = new TenantClient(okapiUrl, TENANT_ID, TOKEN, vertx.createHttpClient());

    final DeploymentOptions options = new DeploymentOptions().setConfig(new JsonObject().put(HTTP_PORT, port));
    vertx.deployVerticle(RestVerticle.class.getName(), options, res -> {
      try {
        TenantAttributes tenantAttributes = new TenantAttributes();
        tenantAttributes.setModuleTo(ModuleName.getModuleName() + TEST_MODULE_VERSION);
        tenantClient.postTenant(tenantAttributes, res2 -> {
          if (res2.result().statusCode() == 204) {
            async.complete();
            return;
          } else if (res2.result().statusCode() == 201) {
            tenantClient.getTenantByOperationId(res2.result().bodyAsJson(TenantJob.class).getId(), 60000, context.asyncAssertSuccess(res3 -> {
              context.assertTrue(res3.bodyAsJson(TenantJob.class).getComplete());
              String error = res3.bodyAsJson(TenantJob.class).getError();
              if (error != null) {
                context.fail("Failed to make post tenant. Received error: " + res3.bodyAsString());
              }
            }));
          } else {
            context.fail("Failed to make post tenant. Received non-2xx status code: " + res2.result().bodyAsString());
          }
          async.complete();
        });
      } catch (Exception e) {
        e.printStackTrace();
      }
    });
  }

  @After
  public void resetWiremock() {
    WireMock.reset();
  }

  @AfterClass
  public static void tearDownClass(final TestContext context) {
    Async async = context.async();
    vertx.close(context.asyncAssertSuccess(res -> {
      if (useExternalDatabase.equals("embedded")) {
        PostgresClient.stopPostgresTester();
      }
      kafkaContainer.close();
      async.complete();
    }));
  }

  @Before
  public void setUp(TestContext context) throws IOException {
    WireMock.configureFor(mockServer.port());

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

    WireMock.stubFor(get(GET_USER_URL + okapiUserIdHeader)
      .willReturn(okJson(userResponse.toString())));

    WireMock.stubFor(post("/change-manager/jobExecutions").withRequestBody(matchingJsonPath("$[?(@.files.size() == 1)]"))
      .willReturn(created().withBody(JsonObject.mapFrom(jobExecutionCreateSingleFile).toString())));
    WireMock.stubFor(post("/change-manager/jobExecutions").withRequestBody(matchingJsonPath("$[?(@.files.size() == 2)]"))
      .willReturn(created().withBody(JsonObject.mapFrom(jobExecutionCreateMultipleFiles).toString())));
    WireMock.stubFor(put(new UrlPathPattern(new RegexPattern("/change-manager/jobExecutions/.*"), true))
      .willReturn(ok()));
    WireMock.stubFor(put(new UrlPathPattern(new RegexPattern("/change-manager/jobExecutions/.{36}/status"), true))
      .willReturn(ok()));
    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern("/change-manager/jobExecutions/.{36}"), true))
      .willReturn(okJson(JsonObject.mapFrom(jobExecution).toString())));
    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern("/change-manager/jobExecutions/.{36}/children"), true))
      .willReturn(okJson(JsonObject.mapFrom(childrenJobExecutions).toString())));
  }

  protected void clearTable(TestContext context) {
    s3Client.createBucketIfNotExists();
    s3Client.remove(s3Client.list(MINIO_BUCKET).toArray(size -> new String[size]));
    PostgresClient.getInstance(vertx, TENANT_ID).delete(FILE_EXTENSIONS_TABLE, new Criterion(), context.asyncAssertSuccess(event1 ->
      PostgresClient.getInstance(vertx, TENANT_ID).delete(UPLOAD_DEFINITIONS_TABLE, new Criterion(), context.asyncAssertSuccess(event2 ->
        PostgresClient.getInstance(vertx).execute("DELETE FROM data_import_global.queue_items;", context.asyncAssertSuccess())
      ))
    ));
  }
}
