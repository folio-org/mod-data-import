package org.folio.rest;

import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static org.assertj.core.api.Assertions.assertThat;
import static org.folio.support.TestUtil.UPLOAD_DEFINITIONS_PATH;

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
import org.folio.support.TestUtil;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class AssembleFileTest extends AbstractRestTest {

  private static final String JOB_EXEC_ID = "90e5a90e-4133-563c-ab33-969b39080c1c";

  @DisplayName("should assemble file from multiple uploaded parts")
  @Test
  void shouldAssembleFile_whenAllPartsUploaded() {
    UploadDefinition definition = createUploadDefinition();

    FileUploadInfo firstPartUploadInfo = getFirstPart("test-name1");
    FileUploadInfo secondPartUploadInfo = getLaterPart(
      firstPartUploadInfo.getKey(), firstPartUploadInfo.getUploadId(), 2);
    FileUploadInfo thirdPartUploadInfo = getLaterPart(
      firstPartUploadInfo.getKey(), firstPartUploadInfo.getUploadId(), 3);

    List<String> tags = new ArrayList<>();
    tags.add(upload(firstPartUploadInfo.getUrl(), 5 * 1024 * 1024));
    tags.add(upload(secondPartUploadInfo.getUrl(), 5 * 1024 * 1024));
    tags.add(upload(thirdPartUploadInfo.getUrl(), 3 * 1024 * 1024));

    given()
      .body(new AssembleFileDto()
        .withKey(firstPartUploadInfo.getKey())
        .withUploadId(firstPartUploadInfo.getUploadId())
        .withTags(tags))
      .pathParam("uploadDefinitionId", definition.getId())
      .pathParam("fileDefinitionId", definition.getFileDefinitions().getFirst().getId())
      .post(TestUtil.ASSEMBLE_PATH)
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);

    UploadDefinition result = getUploadDefinition(definition.getId());

    assertThat(result.getFileDefinitions().getFirst().getSourcePath())
      .isEqualTo(firstPartUploadInfo.getKey());
    assertThat(result.getFileDefinitions().getFirst().getStatus())
      .isEqualTo(FileDefinition.Status.UPLOADED);
  }

  @DisplayName("should return 500 when part upload tags are invalid")
  @Test
  void shouldReturn500_whenPartUploadFailed() {
    UploadDefinition definition = createUploadDefinition();

    FileUploadInfo firstPartUploadInfo = getFirstPart("test-name1");

    given()
      .body(new AssembleFileDto()
        .withKey(firstPartUploadInfo.getKey())
        .withUploadId(firstPartUploadInfo.getUploadId())
        .withTags(List.of("invalid")))
      .pathParam("uploadDefinitionId", definition.getId())
      .pathParam("fileDefinitionId", definition.getFileDefinitions().getFirst().getId())
      .post(TestUtil.ASSEMBLE_PATH)
      .then()
      .statusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);

    UploadDefinition result = getUploadDefinition(definition.getId());

    assertThat(result.getFileDefinitions().getFirst().getSourcePath()).isNull();
    assertThat(result.getFileDefinitions().getFirst().getStatus())
      .isEqualTo(FileDefinition.Status.UPLOADING);
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
      .post(UPLOAD_DEFINITIONS_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .extract().body().as(UploadDefinition.class);
  }

  private UploadDefinition getUploadDefinition(String id) {
    return JsonObject.mapFrom(
      given()
        .get(UPLOAD_DEFINITIONS_PATH + "/" + id)
        .then()
        .statusCode(HttpStatus.SC_OK)
        .extract().as(UploadDefinition.class)
    ).mapTo(UploadDefinition.class);
  }

  private FileUploadInfo getFirstPart(String filename) {
    return given()
      .queryParam("fileName", filename)
      .get(TestUtil.UPLOAD_URL_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .extract().body().as(FileUploadInfo.class);
  }

  private FileUploadInfo getLaterPart(String key, String uploadId, int partNumber) {
    return given()
      .queryParam("key", key)
      .queryParam("uploadId", uploadId)
      .queryParam("partNumber", Integer.toString(partNumber))
      .get(TestUtil.UPLOAD_URL_CONTINUE_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .extract().body().as(FileUploadInfo.class);
  }
}
