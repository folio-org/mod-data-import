package org.folio.rest;

import com.github.tomakehurst.wiremock.client.WireMock;
import io.restassured.RestAssured;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.folio.rest.jaxrs.model.JobExecution;
import org.junit.Test;
import org.junit.runner.RunWith;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.notFound;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static org.hamcrest.Matchers.containsString;

@RunWith(VertxUnitRunner.class)
public class DownloadUrlAPITest extends AbstractRestTest {

  private static final String DOWNLOAD_URL_PATH =
    "/data-import/jobExecutions/{jobExecutionId}/downloadUrl";

  private static final String JOB_EXEC_ID =
    "f26b4519-edfd-5d32-989b-f591b09bd932";

  @Test
  public void testSuccessfulRequest() {
    WireMock.stubFor(
      get("/change-manager/jobExecutions/" + JOB_EXEC_ID)
        .willReturn(
          okJson(
            JsonObject
              .mapFrom(new JobExecution().withSourcePath("test-key-response"))
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
      .statusCode(HttpStatus.SC_OK)
      .body("url", containsString("test-bucket/test-key-response"));
  }

  @Test
  public void testFailedRequest() {
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
}
