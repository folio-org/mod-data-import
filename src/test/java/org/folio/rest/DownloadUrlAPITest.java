package org.folio.rest;

import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.notFound;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.fail;

import com.github.tomakehurst.wiremock.client.WireMock;
import io.restassured.RestAssured;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.http.HttpStatus;
import org.folio.rest.jaxrs.model.AssembleFileDto;
import org.folio.rest.jaxrs.model.FileDefinition;
import org.folio.rest.jaxrs.model.FileUploadInfo;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.UploadDefinition;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class DownloadUrlAPITest extends AbstractRestTest {

  private static final String TEST_KEY = "data-import/test-key-response";

  private static final String DOWNLOAD_URL_PATH =
    "/data-import/jobExecutions/{jobExecutionId}/downloadUrl";

  private static final String DEFINITION_PATH =
    "/data-import/uploadDefinitions";

  private static final String ASSEMBLE_PATH =
    "/data-import/uploadDefinitions/{uploadDefinitionId}/files/{fileDefinitionId}/assembleStorageFile";

  private static final String UPLOAD_URL_PATH = "/data-import/uploadUrl";

  private static final String JOB_EXEC_ID =
    "f26b4519-edfd-5d32-989b-f591b09bd932";

  @Test
  public void testSuccessfulRequest() {
    UploadDefinition definition = createUploadDefinition();

    FileUploadInfo uploadInfo = getFirstPart("test-name");

    List<String> tags = new ArrayList<>();
    tags.add(upload(uploadInfo.getUrl(), 5 * 1024 * 1024));

    AssembleFileDto dto = new AssembleFileDto()
      .withKey(uploadInfo.getKey())
      .withUploadId(uploadInfo.getUploadId())
      .withTags(tags);

    RestAssured
      .given()
      .spec(spec)
      .body(dto)
      .pathParam("uploadDefinitionId", definition.getId())
      .pathParam(
        "fileDefinitionId",
        definition.getFileDefinitions().get(0).getId()
      )
      .when()
      .post(ASSEMBLE_PATH)
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);

    WireMock.stubFor(
      get("/change-manager/jobExecutions/" + JOB_EXEC_ID)
        .willReturn(
          okJson(
            JsonObject
              .mapFrom(new JobExecution().withSourcePath(uploadInfo.getKey()))
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
      .body("url", containsString(uploadInfo.getKey()));
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

  private UploadDefinition createUploadDefinition() {
    WireMock.stubFor(
      put(
        urlPathMatching(
          "/change-manager/jobExecutions/" + JOB_EXEC_ID + "/status"
        )
      )
        .withRequestBody(matchingJsonPath("$.status", equalTo("FILE_UPLOADED")))
        .willReturn(okJson(JsonObject.mapFrom(new JobExecution()).toString()))
    );

    return RestAssured
      .given()
      .spec(spec)
      .body(
        new UploadDefinition()
          .withFileDefinitions(
            Arrays.asList(
              new FileDefinition()
                .withUiKey("ui-key")
                .withName("name.mrc")
                .withSize(10000)
                .withJobExecutionId(JOB_EXEC_ID)
            )
          )
      )
      .when()
      .post(DEFINITION_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .extract()
      .body()
      .as(UploadDefinition.class);
  }

  private FileUploadInfo getFirstPart(String filename) {
    return RestAssured
      .given()
      .spec(spec)
      .when()
      .queryParam("fileName", filename)
      .get(UPLOAD_URL_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .extract()
      .body()
      .as(FileUploadInfo.class);
  }

  private String upload(String url, int size) {
    // unsure how to make this work with restassured...
    try {
      HttpURLConnection con = (HttpURLConnection) new URL(url).openConnection();
      con.setRequestMethod("PUT");
      con.setDoOutput(true);
      OutputStream output = con.getOutputStream();
      output.write(new byte[size]);
      return (con.getHeaderField("eTag"));
    } catch (Exception e) {
      fail(e.getMessage());
      throw new IllegalStateException();
    }
  }
}
