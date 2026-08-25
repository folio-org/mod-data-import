package org.folio.rest;

import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.folio.rest.jaxrs.model.StatusDto.Status.ERROR;
import static org.folio.rest.jaxrs.model.UploadDefinition.Status.COMPLETED;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.restassured.response.ValidatableResponse;
import io.vertx.core.json.Json;
import java.io.File;
import java.net.HttpURLConnection;
import java.net.URI;
import java.util.List;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpStatus;
import org.folio.rest.jaxrs.model.AssembleFileDto;
import org.folio.rest.jaxrs.model.FileDefinition;
import org.folio.rest.jaxrs.model.FileUploadInfo;
import org.folio.rest.jaxrs.model.InitJobExecutionsRsDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.ProcessFilesRqDto;
import org.folio.rest.jaxrs.model.UploadDefinition;
import org.folio.support.AbstractRestTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.awaitility.Awaitility;

class ProcessS3RestTest extends AbstractRestTest {

  // set before BaseRestTest.deployRestVerticle() @BeforeAll so the verticle picks it up
  static {
    System.setProperty("SPLIT_FILES_ENABLED", "true");
  }

  @AfterAll
  static void resetEnv() {
    System.clearProperty("SPLIT_FILES_ENABLED");
  }

  @SuppressWarnings("checkstyle:MethodLength")
  @DisplayName("should complete processing and reach COMPLETED status")
  @Test
  @SneakyThrows
  void shouldCompleteProcessing_whenValidFileUploadedAndProcessed() {
    UploadDefinition uploadDefinition = given()
      .body(new UploadDefinition()
        .withFileDefinitions(List.of(new FileDefinition()
          .withJobExecutionId("9907701d-dd5e-5e9e-8ae6-4dbf7ef10e5d")
          .withUiKey("1.mrc1547160916680")
          .withName("1.mrc")
          .withSize(10))))
      .post("/data-import/uploadDefinitions")
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .extract().body().as(UploadDefinition.class);

    FileUploadInfo uploadInfo = given()
      .queryParam("fileName", uploadDefinition.getFileDefinitions().getFirst().getName())
      .get("/data-import/uploadUrl")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .extract().body().as(FileUploadInfo.class);

    HttpURLConnection con = (HttpURLConnection) URI.create(uploadInfo.getUrl()).toURL().openConnection();
    con.setRequestMethod("PUT");
    con.setDoOutput(true);
    con.getOutputStream().write(
      FileUtils.readFileToByteArray(new File(
        getClass().getClassLoader().getResource(
          uploadDefinition.getFileDefinitions().getFirst().getName()
        ).toURI()
      ))
    );
    String etag = con.getHeaderField("eTag");

    given()
      .body(new AssembleFileDto()
        .withKey(uploadInfo.getKey())
        .withUploadId(uploadInfo.getUploadId())
        .withTags(List.of(etag)))
      .pathParam("uploadDefinitionId", uploadDefinition.getId())
      .pathParam("fileDefinitionId", uploadDefinition.getFileDefinitions().getFirst().getId())
      .post("/data-import/uploadDefinitions/{uploadDefinitionId}/files/{fileDefinitionId}/assembleStorageFile")
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);

    uploadDefinition.getFileDefinitions().forEach(fd -> fd.setSourcePath(uploadInfo.getKey()));

    WIRE_MOCK.stubFor(WireMock.post("/change-manager/jobExecutions")
      .willReturn(WireMock.created().withBody(Json.encode(new InitJobExecutionsRsDto()
        .withJobExecutions(List.of(new JobExecution()
          .withId("445308a4-d3e0-562e-a7fe-28b2ef5ceb23")
          .withSourcePath(uploadInfo.getKey())))))));

    WIRE_MOCK.stubFor(WireMock.get(urlPathMatching("/change-manager/jobExecutions/[^/]*"))
      .willReturn(WireMock.okJson(Json.encode(
        new JobExecution().withId("3ed691e7-df5b-58e8-aaec-a18962a40744")))));

    given()
      .body(Json.encodePrettily(new ProcessFilesRqDto()
        .withUploadDefinition(uploadDefinition)
        .withJobProfileInfo(new JobProfileInfo()
          .withId("3aa9cdff-737a-5d08-916f-94e862c0ae5f")
          .withDataType(JobProfileInfo.DataType.MARC))))
      .pathParam("uploadDefinitionId", uploadDefinition.getId())
      .post("/data-import/uploadDefinitions/{uploadDefinitionId}/processFiles")
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);

    await().atMost(60, SECONDS).pollInterval(5, SECONDS).until(() -> {
      ValidatableResponse response = given()
        .get("/data-import/uploadDefinitions/" + uploadDefinition.getId())
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("metaJobExecutionId", notNullValue())
        .body("id", notNullValue());
      String status = response.extract().body().jsonPath().getString("status");
      return COMPLETED.name().equals(status);
    });
  }

  @DisplayName("should return 204 even when processing fails and eventually reach ERROR status")
  @Test
  void shouldReturn204_whenProcessingFails() {
    WIRE_MOCK.stubFor(put(new UrlPathPattern(new RegexPattern("/change-manager/jobExecutions/.*"), true))
      .willReturn(ok()));

    UploadDefinition uploadDefinition = given()
      .body(new UploadDefinition()
        .withFileDefinitions(List.of(new FileDefinition()
          .withJobExecutionId("9907701d-dd5e-5e9e-8ae6-4dbf7ef10e5d")
          .withUiKey("1.mrc1547160916680")
          .withName("1.mrc")
          .withSize(10))))
      .post("/data-import/uploadDefinitions")
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .extract().body().as(UploadDefinition.class);

    given()
      .body(new ProcessFilesRqDto()
        .withUploadDefinition(uploadDefinition)
        .withJobProfileInfo(new JobProfileInfo()
          .withId("3aa9cdff-737a-5d08-916f-94e862c0ae5f")
          .withDataType(JobProfileInfo.DataType.MARC)))
      .pathParam("uploadDefinitionId", uploadDefinition.getId())
      .post("/data-import/uploadDefinitions/{uploadDefinitionId}/processFiles")
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);

    Awaitility.await().untilAsserted(() ->
      given()
        .get("/data-import/uploadDefinitions/" + uploadDefinition.getId())
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("metaJobExecutionId", is(uploadDefinition.getMetaJobExecutionId()))
        .body("id", notNullValue())
        .body("status", is(ERROR.name())));
  }
}
