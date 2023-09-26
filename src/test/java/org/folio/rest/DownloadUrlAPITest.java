package org.folio.rest;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.notFound;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static org.hamcrest.Matchers.containsString;

import com.github.tomakehurst.wiremock.client.WireMock;
import io.restassured.RestAssured;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.io.ByteArrayInputStream;
import org.apache.http.HttpStatus;
import org.folio.rest.jaxrs.model.JobExecution;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class DownloadUrlAPITest extends AbstractRestTest {

  private static final String TEST_KEY = "data-import/test-key-response";

  private static final String DOWNLOAD_URL_PATH =
    "/data-import/jobExecutions/{jobExecutionId}/downloadUrl";

  private static final String JOB_EXEC_ID =
    "f26b4519-edfd-5d32-989b-f591b09bd932";

  @After
  public void cleanupS3() {
    s3Client.remove(TEST_KEY);
  }

  @Test
  public void testSuccessfulRequest() {
    WireMock.stubFor(
      get("/change-manager/jobExecutions/" + JOB_EXEC_ID)
        .willReturn(
          okJson(
            JsonObject
              .mapFrom(new JobExecution().withSourcePath(TEST_KEY))
              .toString()
          )
        )
    );

    s3Client.write(TEST_KEY, new ByteArrayInputStream(new byte[5]));

    RestAssured
      .given()
      .spec(spec)
      .when()
      .pathParam("jobExecutionId", JOB_EXEC_ID)
      .get(DOWNLOAD_URL_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("url", containsString("test-bucket/data-import/test-key-response"));
  }

  @Test
  public void testOutOfScopeRequest() {
    WireMock.stubFor(
      get("/change-manager/jobExecutions/" + JOB_EXEC_ID)
        .willReturn(
          okJson(
            JsonObject
              .mapFrom(
                new JobExecution()
                  .withSourcePath("not-correct-prefix/test-key-response")
              )
              .toString()
          )
        )
    );

    RestAssured
      .given()
      .spec(spec)
      .when()
      .pathParam("jobExecutionId", JOB_EXEC_ID)
      .get(DOWNLOAD_URL_PATH)
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void testMissingJobExecutionRequest() {
    WireMock.stubFor(
      get("/change-manager/jobExecutions/" + JOB_EXEC_ID).willReturn(notFound())
    );

    RestAssured
      .given()
      .spec(spec)
      .when()
      .pathParam("jobExecutionId", JOB_EXEC_ID)
      .get(DOWNLOAD_URL_PATH)
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void testMissingFileFromS3Request() {
    WireMock.stubFor(
      get("/change-manager/jobExecutions/" + JOB_EXEC_ID)
        .willReturn(
          okJson(
            JsonObject
              .mapFrom(new JobExecution().withSourcePath(TEST_KEY))
              .toString()
          )
        )
    );

    RestAssured
      .given()
      .spec(spec)
      .when()
      .pathParam("jobExecutionId", JOB_EXEC_ID)
      .get(DOWNLOAD_URL_PATH)
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }
}
