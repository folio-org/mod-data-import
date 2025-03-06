package org.folio.rest;

import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.folio.rest.jaxrs.model.UploadDefinition.Status.COMPLETED;
import static org.hamcrest.Matchers.notNullValue;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.restassured.RestAssured;
import io.restassured.response.ValidatableResponse;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
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
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class ProcessS3APITest extends AbstractRestTest {

  @BeforeClass
  public static void configureEnv() {
    System.setProperty("SPLIT_FILES_ENABLED", "true");
  }

  @Test
  public void testProcessingSuccess() throws IOException {
    UploadDefinition uploadDefinition = RestAssured
      .given()
      .spec(spec)
      .body(
        new UploadDefinition()
          .withFileDefinitions(
            Arrays.asList(
              new FileDefinition()
                .withJobExecutionId("9907701d-dd5e-5e9e-8ae6-4dbf7ef10e5d")
                .withUiKey("1.mrc1547160916680")
                .withName("1.mrc")
                .withSize(10)
            )
          )
      )
      .when()
      .post("/data-import/uploadDefinitions")
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .extract()
      .body()
      .as(UploadDefinition.class);

    FileUploadInfo uploadInfo = RestAssured
      .given()
      .spec(spec)
      .when()
      .queryParam(
        "fileName",
        uploadDefinition.getFileDefinitions().get(0).getName()
      )
      .get("/data-import/uploadUrl")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .extract()
      .body()
      .as(FileUploadInfo.class);

    HttpURLConnection con = (HttpURLConnection) new URL(uploadInfo.getUrl())
      .openConnection();
    con.setRequestMethod("PUT");
    con.setDoOutput(true);
    con
      .getOutputStream()
      .write(
        FileUtils.readFileToByteArray(
          new File(
            getClass()
              .getClassLoader()
              .getResource(
                uploadDefinition.getFileDefinitions().get(0).getName()
              )
              .getFile()
          )
        )
      );
    String eTag = con.getHeaderField("eTag");

    RestAssured
      .given()
      .spec(spec)
      .body(
        new AssembleFileDto()
          .withKey(uploadInfo.getKey())
          .withUploadId(uploadInfo.getUploadId())
          .withTags(Arrays.asList(eTag))
      )
      .pathParam("uploadDefinitionId", uploadDefinition.getId())
      .pathParam(
        "fileDefinitionId",
        uploadDefinition.getFileDefinitions().get(0).getId()
      )
      .when()
      .post(
        "/data-import/uploadDefinitions/{uploadDefinitionId}/files/{fileDefinitionId}/assembleStorageFile"
      )
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);

    uploadDefinition
      .getFileDefinitions()
      .forEach(fd -> fd.setSourcePath(uploadInfo.getKey()));

    WireMock.stubFor(
      WireMock
        .post("/change-manager/jobExecutions")
        .willReturn(
          WireMock
            .created()
            .withBody(
              JsonObject
                .mapFrom(
                  new InitJobExecutionsRsDto()
                    .withJobExecutions(
                      Arrays.asList(
                        new JobExecution()
                          .withId("445308a4-d3e0-562e-a7fe-28b2ef5ceb23")
                          .withSourcePath(uploadInfo.getKey())
                      )
                    )
                )
                .encode()
            )
        )
    );

    WireMock.stubFor(
      WireMock
        .get(urlPathMatching("/change-manager/jobExecutions/[^/]*"))
        .willReturn(
          WireMock.okJson(
            JsonObject
              .mapFrom(
                new JobExecution()
                  .withId("3ed691e7-df5b-58e8-aaec-a18962a40744")
              )
              .encode()
          )
        )
    );

    RestAssured
      .given()
      .spec(spec)
      .body(
        new ProcessFilesRqDto()
          .withUploadDefinition(uploadDefinition)
          .withJobProfileInfo(
            new JobProfileInfo()
              .withId("3aa9cdff-737a-5d08-916f-94e862c0ae5f")
              .withDataType(JobProfileInfo.DataType.MARC)
          )
      )
      .pathParam("uploadDefinitionId", uploadDefinition.getId())
      .when()
      .post("/data-import/uploadDefinitions/{uploadDefinitionId}/processFiles")
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);

    await().atMost(60, SECONDS).pollInterval(5, SECONDS).until(() -> {
      ValidatableResponse response = RestAssured.given()
        .spec(spec)
        .when()
        .get("/data-import/uploadDefinitions/" + uploadDefinition.getId())
        .then()
        .statusCode(HttpStatus.SC_OK)
        .body("metaJobExecutionId", notNullValue())
        .body("id", notNullValue());

      String status = response.extract().body().jsonPath().getString("status");
      return COMPLETED.name().equals(status);
    });
  }

  @Test
  public void testReturnSuccessEvenIfProcessingFailing() {
    WireMock.stubFor(put(new UrlPathPattern(new RegexPattern("/change-manager/jobExecutions/.*"), true))
      .willReturn(ok()));

    RestAssured
      .given()
      .spec(spec)
      .body(
        new ProcessFilesRqDto()
          .withUploadDefinition(
            new UploadDefinition()
              .withId("cfa58c5f-6911-53bb-8ce1-73c8bd4e1cef")
          )
          .withJobProfileInfo(
            new JobProfileInfo()
              .withId("3aa9cdff-737a-5d08-916f-94e862c0ae5f")
              .withDataType(JobProfileInfo.DataType.MARC)
          )
      )
      .pathParam("uploadDefinitionId", "cfa58c5f-6911-53bb-8ce1-73c8bd4e1cef")
      .when()
      .post("/data-import/uploadDefinitions/{uploadDefinitionId}/processFiles")
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT);
  }

  @AfterClass
  public static void resetEnv() {
    System.clearProperty("SPLIT_FILES_ENABLED");
  }
}
