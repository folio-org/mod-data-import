package org.folio.rest;

import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;

import com.github.tomakehurst.wiremock.client.WireMock;
import io.restassured.RestAssured;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import org.apache.http.HttpStatus;
import org.folio.rest.jaxrs.model.FileDefinition;
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

  private static FileDefinition FILE_DEFINITION = new FileDefinition()
    .withJobExecutionId("9907701d-dd5e-5e9e-8ae6-4dbf7ef10e5d")
    .withUiKey("1.mrc1547160916680")
    .withName("1.mrc")
    .withSourcePath("key/s3-key")
    .withSize(209);

  @BeforeClass
  public static void configureEnv() {
    System.setProperty("SPLIT_FILES_ENABLED", "true");
  }

  @Test
  public void testProcessing() throws IOException {
    UploadDefinition uploadDefinition = new UploadDefinition()
      .withMetaJobExecutionId("53db5d43-9f47-521f-8a1a-ffb9dcac11c7")
      .withFileDefinitions(Collections.singletonList(FILE_DEFINITION));

    uploadDefinition.setId(
      RestAssured
        .given()
        .spec(spec)
        .body(uploadDefinition)
        .when()
        .post("/data-import/uploadDefinitions")
        .then()
        .statusCode(HttpStatus.SC_CREATED)
        .extract()
        .body()
        .jsonPath()
        .get("id")
    );

    s3Client.write(
      FILE_DEFINITION.getSourcePath(),
      new FileInputStream(
        new File(
          getClass()
            .getClassLoader()
            .getResource(FILE_DEFINITION.getName())
            .getFile()
        )
      )
    );

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
                          .withSourcePath("key/s3-key")
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
  }

  @Test
  public void testProcessingFailure() throws IOException {
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
      .statusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
  }

  @AfterClass
  public static void resetEnv() {
    System.clearProperty("SPLIT_FILES_ENABLED");
  }
}
