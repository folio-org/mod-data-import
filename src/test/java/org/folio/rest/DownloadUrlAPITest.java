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
import java.util.Arrays;
import java.util.List;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.compress.utils.ByteUtils;
import org.apache.http.HttpStatus;
import org.folio.rest.jaxrs.model.JobExecution;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.testcontainers.images.builder.Transferable;

@Log4j2
@RunWith(VertxUnitRunner.class)
public class DownloadUrlAPITest extends AbstractRestTest {

  private static final String TEST_KEY = "data-import/test-key-response";

  private static final String DOWNLOAD_URL_PATH =
    "/data-import/jobExecutions/{jobExecutionId}/downloadUrl";

  private static final String JOB_EXEC_ID =
    "f26b4519-edfd-5d32-989b-f591b09bd932";

  @After
  public void cleanupS3() {
    log.info(
      "===================================CLEANUP==================================="
    );
    s3Client.remove(TEST_KEY);
  }

  @Test
  public void testRunner() {
    List<List<Integer>> cases = Arrays.asList(
      Arrays.asList(4, 2, 3, 1),
      Arrays.asList(2, 1, 3, 4),
      Arrays.asList(3, 1, 2, 4),
      Arrays.asList(1, 3, 2, 4),
      Arrays.asList(2, 3, 1, 4),
      Arrays.asList(3, 2, 1, 4),
      Arrays.asList(3, 2, 4, 1),
      Arrays.asList(2, 3, 4, 1),
      Arrays.asList(4, 3, 2, 1),
      Arrays.asList(3, 4, 2, 1),
      Arrays.asList(2, 4, 3, 1),
      Arrays.asList(1, 2, 3, 4),
      Arrays.asList(4, 1, 3, 2),
      Arrays.asList(1, 4, 3, 2),
      Arrays.asList(3, 4, 1, 2),
      Arrays.asList(4, 3, 1, 2),
      Arrays.asList(1, 3, 4, 2),
      Arrays.asList(3, 1, 4, 2),
      Arrays.asList(2, 1, 4, 3),
      Arrays.asList(1, 2, 4, 3),
      Arrays.asList(4, 2, 1, 3),
      Arrays.asList(2, 4, 1, 3),
      Arrays.asList(1, 4, 2, 3),
      Arrays.asList(4, 1, 2, 3)
    );

    for (List<Integer> set : cases) {
      log.info(
        "===================================TEST SET==================================="
      );

      log.info("Running order {}", set);

      for (Integer i : set) {
        switch (i) {
          case 1:
            testMissingFileFromS3Request();
            break;
          case 2:
            testOutOfScopeRequest();
            break;
          case 3:
            testMissingJobExecutionRequest();
            break;
          case 4:
            testSuccessfulRequest();
            break;
        }
        cleanupS3();
        resetWiremock();
      }
    }
  }

  public void testSuccessfulRequest() {
    log.info(
      "===================================testSuccessfulRequest==================================="
    );

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

    s3Client.write(
      TEST_KEY,
      new ByteArrayInputStream("test content".getBytes())
    );

    log.info(s3Client.list(""));

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

  public void testOutOfScopeRequest() {
    log.info(
      "===================================testOutOfScopeRequest==================================="
    );
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

  public void testMissingJobExecutionRequest() {
    log.info(
      "===================================testMissingJobExecutionRequest==================================="
    );
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

  public void testMissingFileFromS3Request() {
    log.info(
      "===================================testMissingFileFromS3Request==================================="
    );
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
