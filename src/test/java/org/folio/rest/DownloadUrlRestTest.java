package org.folio.rest;

import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.notFound;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static org.folio.support.TestUtil.ASSEMBLE_PATH;
import static org.folio.support.TestUtil.DEFINITION_PATH;
import static org.folio.support.TestUtil.DOWNLOAD_URL_PATH;
import static org.folio.support.TestUtil.UPLOAD_URL_PATH;
import static org.hamcrest.Matchers.containsString;

import io.vertx.core.json.JsonObject;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.http.HttpStatus;
import org.folio.rest.jaxrs.model.AssembleFileDto;
import org.folio.rest.jaxrs.model.FileDefinition;
import org.folio.rest.jaxrs.model.FileUploadInfo;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.UploadDefinition;
import org.folio.support.AbstractRestTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DownloadUrlRestTest extends AbstractRestTest {

  private static final String TEST_KEY = "data-import/test-key-response";

  private static final String JOB_EXEC_ID = "f26b4519-edfd-5d32-989b-f591b09bd932";

  @DisplayName("should return a pre-signed download URL when file exists in S3")
  @Test
  void shouldReturnDownloadUrl_whenFileExistsInS3() {
    UploadDefinition definition = createUploadDefinition();

    FileUploadInfo uploadInfo = given()
      .queryParam("fileName", "test-name")
      .get(UPLOAD_URL_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .extract().body().as(FileUploadInfo.class);

    List<String> tags = new ArrayList<>();
    tags.add(upload(uploadInfo.getUrl(), 5 * 1024 * 1024));

    given()
      .body(new AssembleFileDto()
        .withKey(uploadInfo.getKey())
        .withUploadId(uploadInfo.getUploadId())
        .withTags(tags))
      .pathParam("uploadDefinitionId", definition.getId())
      .pathParam("fileDefinitionId", definition.getFileDefinitions().getFirst().getId())
      .post(ASSEMBLE_PATH)
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);

    WIRE_MOCK.stubFor(get("/change-manager/jobExecutions/" + JOB_EXEC_ID)
      .willReturn(okJson(JsonObject.mapFrom(
        new JobExecution().withSourcePath(uploadInfo.getKey())
      ).toString())));

    given()
      .pathParam("jobExecutionId", JOB_EXEC_ID)
      .get(DOWNLOAD_URL_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("url", containsString(uploadInfo.getKey()));
  }

  @DisplayName("should return 404 when file key is outside the permitted path")
  @Test
  void shouldReturn404_whenKeyOutsidePermittedPath() {
    WIRE_MOCK.stubFor(get("/change-manager/jobExecutions/" + JOB_EXEC_ID)
      .willReturn(okJson(JsonObject.mapFrom(
        new JobExecution().withSourcePath("not-correct-prefix/test-key-response")
      ).toString())));

    given()
      .pathParam("jobExecutionId", JOB_EXEC_ID)
      .get(DOWNLOAD_URL_PATH)
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @DisplayName("should return 404 when job execution is not found")
  @Test
  void shouldReturn404_whenJobExecutionNotFound() {
    WIRE_MOCK.stubFor(get("/change-manager/jobExecutions/" + JOB_EXEC_ID)
      .willReturn(notFound()));

    given()
      .pathParam("jobExecutionId", JOB_EXEC_ID)
      .get(DOWNLOAD_URL_PATH)
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @DisplayName("should return 404 when file is absent from S3")
  @Test
  void shouldReturn404_whenFileAbsentFromS3() {
    WIRE_MOCK.stubFor(get("/change-manager/jobExecutions/" + JOB_EXEC_ID)
      .willReturn(okJson(JsonObject.mapFrom(new JobExecution().withSourcePath(TEST_KEY)).toString())));

    given()
      .pathParam("jobExecutionId", JOB_EXEC_ID)
      .get(DOWNLOAD_URL_PATH)
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  private UploadDefinition createUploadDefinition() {
    WIRE_MOCK.stubFor(put(urlPathMatching(
      "/change-manager/jobExecutions/" + JOB_EXEC_ID + "/status"))
      .withRequestBody(matchingJsonPath("$.status", equalTo("FILE_UPLOADED")))
      .willReturn(okJson(JsonObject.mapFrom(new JobExecution()).toString())));

    return given()
      .body(new UploadDefinition()
        .withFileDefinitions(Collections.singletonList(new FileDefinition()
          .withUiKey("ui-key")
          .withName("name.mrc")
          .withSize(10000)
          .withJobExecutionId(JOB_EXEC_ID))))
      .post(DEFINITION_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .extract().body().as(UploadDefinition.class);
  }
}
