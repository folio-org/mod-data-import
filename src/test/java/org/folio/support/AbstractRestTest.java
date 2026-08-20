package org.folio.support;

import static com.github.tomakehurst.wiremock.client.WireMock.created;
import static com.github.tomakehurst.wiremock.client.WireMock.delete;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.noContent;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static org.folio.support.TestUtil.MINIO_BUCKET;
import static org.folio.support.TestUtil.SUB_PATH;
import static org.folio.support.TestUtil.TENANT_ID;
import static org.folio.support.TestUtil.clearAllTables;
import static org.junit.jupiter.api.Assertions.fail;

import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.restassured.builder.RequestSpecBuilder;
import io.restassured.specification.RequestSpecification;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import org.folio.dataimport.testsupport.rest.BaseRestTest;
import org.folio.dataimport.testsupport.s3.S3Extension;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.jaxrs.model.InitJobExecutionsRsDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionDto;
import org.folio.rest.jaxrs.model.JobExecutionDtoCollection;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.tools.utils.ModuleName;
import org.folio.s3.client.FolioS3Client;
import org.folio.spring.SpringContextUtil;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * Base class for mod-data-import REST integration tests.
 *
 * <p>Boots the module against shared PostgreSQL, Kafka and S3 containers provided by the
 * registered JUnit 5 extensions and the {@link BaseRestTest} superclass. Common WireMock stubs
 * (user lookup, change-manager endpoints) are applied before each test so subclasses only need to
 * stub the endpoints specific to their scenario.
 */
@ExtendWith(VertxExtension.class)
@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = ApplicationTestConfig.class)
public abstract class AbstractRestTest extends BaseRestTest {

  @RegisterExtension
  protected static final S3Extension S3 = new S3Extension(MINIO_BUCKET, SUB_PATH);
  protected static final String TOKEN = "token";
  protected static FolioS3Client s3Client;
  private static final String USER_ID = UUID.randomUUID().toString();
  private static final String GET_USER_URL = "/users\\?query=id==" + USER_ID;
  protected RequestSpecification specUpload;
  private final JsonObject userResponse = new JsonObject()
    .put("users",
      new JsonArray().add(new JsonObject()
        .put("username", "diku_admin")
        .put("personal", new JsonObject().put("firstName", "DIKU").put("lastName", "ADMINISTRATOR"))))
    .put("totalRecords", 1);

  private final JobExecutionDto jobExecution = new JobExecutionDto()
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

  private final JobExecutionDtoCollection childrenJobExecutions = new JobExecutionDtoCollection()
    .withJobExecutions(Arrays.asList(
      new JobExecutionDto().withId(UUID.randomUUID().toString())
        .withSubordinationType(JobExecutionDto.SubordinationType.CHILD)
        .withStatus(JobExecutionDto.Status.NEW),
      new JobExecutionDto().withId(UUID.randomUUID().toString())
        .withSubordinationType(JobExecutionDto.SubordinationType.CHILD)
        .withStatus(JobExecutionDto.Status.NEW)))
    .withTotalRecords(2);

  private final InitJobExecutionsRsDto jobExecutionCreateSingleFile = new InitJobExecutionsRsDto()
    .withParentJobExecutionId(UUID.randomUUID().toString())
    .withJobExecutions(Collections.singletonList(
      new JobExecution().withId(UUID.randomUUID().toString())
        .withSourcePath("CornellFOLIOExemplars_Bibs(1).mrc")));

  private final InitJobExecutionsRsDto jobExecutionCreateMultipleFiles = new InitJobExecutionsRsDto()
    .withParentJobExecutionId(UUID.randomUUID().toString())
    .withJobExecutions(Arrays.asList(
      new JobExecution().withId(UUID.randomUUID().toString())
        .withSourcePath("CornellFOLIOExemplars_Bibs(1).mrc"),
      new JobExecution().withId(UUID.randomUUID().toString())
        .withSourcePath("CornellFOLIOExemplars.mrc")));

  @Override
  protected String getModuleName() {
    return ModuleName.getModuleName() + "-1.0.0";
  }

  @Override
  protected Map<String, String> getExtraSpecHeaders() {
    return Map.of(XOkapiHeaders.USER_ID, USER_ID);
  }

  protected String upload(String url, int size) {
    try {
      HttpURLConnection con = (HttpURLConnection) new URI(url).toURL().openConnection();
      con.setRequestMethod("PUT");
      con.setDoOutput(true);
      OutputStream output = con.getOutputStream();
      output.write(new byte[size]);
      return con.getHeaderField("eTag");
    } catch (Exception e) {
      fail(e.getMessage());
      throw new IllegalStateException();
    }
  }

  @BeforeEach
  protected void setUp(VertxTestContext testContext) {
    WIRE_MOCK.resetAll();
    stubCommonEndpoints();
    clearTables(testContext);
  }

  /**
   * Clears all module tables and the S3 bucket between tests.
   *
   * <p>Override to skip or restrict cleanup for specific test classes (e.g. default-profile tests
   * that should not delete defaults).
   *
   * @param testContext the current test context
   */
  protected void clearTables(VertxTestContext testContext) {
    clearS3();
    clearAllTables(vertx).onComplete(testContext.succeedingThenComplete());
  }

  /**
   * Removes all objects from the shared S3 bucket.
   */
  protected void clearS3() {
    var keys = s3Client.list(MINIO_BUCKET);
    if (!keys.isEmpty()) {
      s3Client.remove(keys.toArray(new String[0]));
    }
  }

  @BeforeAll
  void setUpClass() {
    s3Client = S3.buildS3Client();
    s3Client.createBucketIfNotExists();
    specUpload = new RequestSpecBuilder()
      .setContentType("application/octet-stream")
      .setBaseUri(connectionUrl)
      .addHeader(XOkapiHeaders.TENANT, TENANT_ID)
      .addHeader(XOkapiHeaders.USER_ID, USER_ID)
      .addHeader(XOkapiHeaders.URL, mockServerUrl())
      .addHeader("Accept", "text/plain, application/json")
      .build();
    SpringContextUtil.autowireDependenciesFromFirstContext(this, vertx);
  }

  private void stubCommonEndpoints() {
    stubGetJson(GET_USER_URL, userResponse.toString());

    WIRE_MOCK.stubFor(post("/change-manager/jobExecutions")
      .withRequestBody(matchingJsonPath("$[?(@.files.size() == 1)]"))
      .willReturn(created().withBody(JsonObject.mapFrom(jobExecutionCreateSingleFile).toString())));
    WIRE_MOCK.stubFor(post("/change-manager/jobExecutions")
      .withRequestBody(matchingJsonPath("$[?(@.files.size() == 2)]"))
      .willReturn(created().withBody(JsonObject.mapFrom(jobExecutionCreateMultipleFiles).toString())));
    WIRE_MOCK.stubFor(put(new UrlPathPattern(new RegexPattern("/change-manager/jobExecutions/.*"), true))
      .willReturn(ok()));
    WIRE_MOCK.stubFor(put(new UrlPathPattern(new RegexPattern("/change-manager/jobExecutions/.{36}/status"), true))
      .willReturn(ok()));
    WIRE_MOCK.stubFor(delete(new UrlPathPattern(new RegexPattern("/change-manager/jobExecutions/.{36}/records"), true))
      .willReturn(noContent()));
    WIRE_MOCK.stubFor(get(new UrlPathPattern(new RegexPattern("/change-manager/jobExecutions/.{36}"), true))
      .willReturn(okJson(JsonObject.mapFrom(jobExecution).toString())));
    WIRE_MOCK.stubFor(get(new UrlPathPattern(new RegexPattern("/change-manager/jobExecutions/.{36}/children"), true))
      .willReturn(okJson(JsonObject.mapFrom(childrenJobExecutions).toString())));
  }
}
