package org.folio.rest;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import com.jayway.restassured.RestAssured;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpStatus;
import org.drools.core.util.StringUtils;
import org.folio.rest.jaxrs.model.FileDefinition;
import org.folio.rest.jaxrs.model.JobProfile;
import org.folio.rest.jaxrs.model.ProcessFilesRqDto;
import org.folio.rest.jaxrs.model.UploadDefinition;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Objects;
import java.util.UUID;

import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertTrue;

@RunWith(VertxUnitRunner.class)
public class UploadDefinitionAPITest extends AbstractRestTest {

  private static final String DEFINITION_PATH = "/data-import/uploadDefinitions";
  private static final String FILE_PATH = "/files";
  private static final String PROCESS_FILE_IMPORT_PATH = "/processFiles";

  private static FileDefinition file1 = new FileDefinition()
    .withUiKey("CornellFOLIOExemplars_Bibs(1).mrc.md1547160916680")
    .withName("CornellFOLIOExemplars_Bibs(1).mrc")
    .withSize(209);

  private static FileDefinition file2 = new FileDefinition()
    .withUiKey("CornellFOLIOExemplars.mrc.md1547160916680")
    .withName("CornellFOLIOExemplars.mrc")
    .withSize(209);

  private static FileDefinition file3 = new FileDefinition()
    .withUiKey("CornellFOLIOExemplars.mrc.md1547160916680")
    .withName("CornellFOLIOExemplars.mrc")
    .withSize(Integer.MAX_VALUE);

  private static FileDefinition file4 = new FileDefinition()
    .withUiKey("CornellFOLIOExemplars1.mrc.md1547160916681")
    .withName("CornellFOLIOExemplars1.mrc")
    .withSize(Integer.MAX_VALUE);

  private static UploadDefinition uploadDef1 = new UploadDefinition()
    .withFileDefinitions(Collections.singletonList(file1));

  private static UploadDefinition uploadDef2 = new UploadDefinition()
    .withFileDefinitions(Collections.singletonList(file1));

  private static UploadDefinition uploadDef3 = new UploadDefinition()
    .withFileDefinitions(Collections.singletonList(file1));

  private static UploadDefinition uploadDef4 = new UploadDefinition()
    .withFileDefinitions(Arrays.asList(file1, file2));

  private static UploadDefinition uploadDef5 = new UploadDefinition()
    .withFileDefinitions(Arrays.asList(file3, file4));

  @After
  public void cleanUpAfterTest() throws IOException {
    FileUtils.deleteDirectory(new File("./storage"));
  }

  @Test
  public void uploadDefinitionCreate() {
    RestAssured.given()
      .spec(spec)
      .body(uploadDef1)
      .when()
      .post(DEFINITION_PATH)
      .then()
      .log().all()
      .statusCode(HttpStatus.SC_CREATED)
      .body("metaJobExecutionId", notNullValue())
      .body("id", notNullValue())
      .body("status", is(UploadDefinition.Status.NEW.name()))
      .body("fileDefinitions[0].status", is(FileDefinition.Status.NEW.name()));
  }

  @Test
  public void uploadDefinitionGet() {
    String id = RestAssured.given()
      .spec(spec)
      .body(uploadDef1)
      .when()
      .post(DEFINITION_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .log().all().extract().body().jsonPath().get("id");
    RestAssured.given()
      .spec(spec)
      .when()
      .get(DEFINITION_PATH + "?query=id==" + id)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(1))
      .log().all();
  }

  @Test
  public void uploadDefinitionGetNotFound() {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(DEFINITION_PATH + "?query=id==" + UUID.randomUUID().toString())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(0))
      .log().all();
  }

  @Test
  public void uploadDefinitionGetById() {
    String id = RestAssured.given()
      .spec(spec)
      .body(uploadDef2)
      .when()
      .post(DEFINITION_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .log().all().extract().body().jsonPath().get("id");
    RestAssured.given()
      .spec(spec)
      .when()
      .get(DEFINITION_PATH + "/" + id)
      .then()
      .log().all()
      .statusCode(HttpStatus.SC_OK)
      .body("metaJobExecutionId", notNullValue())
      .body("id", notNullValue())
      .body("status", is(UploadDefinition.Status.NEW.name()))
      .body("fileDefinitions[0].status", is(FileDefinition.Status.NEW.name()));
  }

  @Test
  public void uploadDefinitionGetByIdNotFound() {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(DEFINITION_PATH + "/" + UUID.randomUUID())
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND)
      .log().all();
  }

  @Test
  public void uploadDefinitionUpdate() {
    UploadDefinition uploadDefinition = RestAssured.given()
      .spec(spec)
      .body(uploadDef3)
      .when()
      .post(DEFINITION_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .log().all()
      .extract().body().as(UploadDefinition.class);
    uploadDefinition.setStatus(UploadDefinition.Status.LOADED);
    RestAssured.given()
      .spec(spec)
      .body(uploadDefinition)
      .when()
      .put(DEFINITION_PATH + "/" + uploadDefinition.getId())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .log().all()
      .body("status", is(UploadDefinition.Status.LOADED.name()));
  }

  @Test
  public void uploadDefinitionUpdateNotFound() {
    RestAssured.given()
      .spec(spec)
      .body(uploadDef3)
      .when()
      .put(DEFINITION_PATH + "/" + UUID.randomUUID().toString())
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND)
      .log().all();
  }

  @Test
  public void fileUpload() throws IOException {
    UploadDefinition uploadDefinition = RestAssured.given()
      .spec(spec)
      .body(uploadDef3)
      .when()
      .post(DEFINITION_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .log().all()
      .extract().body().as(UploadDefinition.class);
    String uploadDefId = uploadDefinition.getId();
    String fileId = uploadDefinition.getFileDefinitions().get(0).getId();

    ClassLoader classLoader = getClass().getClassLoader();
    File file = new File(Objects.requireNonNull(classLoader.getResource("CornellFOLIOExemplars_Bibs.mrc")).getFile());
    UploadDefinition uploadDefinition1 = RestAssured.given()
      .spec(specUpload)
      .when()
      .body(FileUtils.openInputStream(file))
      .post(DEFINITION_PATH + "/" + uploadDefId + FILE_PATH + "/" + fileId)
      .then()
      .log().all()
      .statusCode(HttpStatus.SC_OK)
      .body("status", is(UploadDefinition.Status.LOADED.name()))
      .body("fileDefinitions[0].status", is(FileDefinition.Status.UPLOADED.name()))
      .body("fileDefinitions.uploadedDate", notNullValue())
      .extract().body().as(UploadDefinition.class);
    String path = uploadDefinition1.getFileDefinitions().get(0).getSourcePath();
    File file2 = new File(path);
    assertTrue(FileUtils.contentEquals(file, file2));
  }

  @Test
  public void fileUploadNotFound() {
    ClassLoader classLoader = getClass().getClassLoader();
    File file = new File(Objects.requireNonNull(classLoader.getResource("CornellFOLIOExemplars_Bibs.mrc")).getFile());
    RestAssured.given()
      .spec(specUpload)
      .when()
      .body(file)
      .post(DEFINITION_PATH + "/"
        + UUID.randomUUID().toString()
        + FILE_PATH + "/"
        + UUID.randomUUID().toString())
      .then()
      .log().all()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @Test
  public void fileDelete() {
    UploadDefinition uploadDefinition = RestAssured.given()
      .spec(spec)
      .body(uploadDef3)
      .when()
      .post(DEFINITION_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .log().all()
      .extract().body().as(UploadDefinition.class);
    RestAssured.given()
      .spec(spec)
      .when()
      .delete(DEFINITION_PATH + "/" + uploadDefinition.getId()
        + FILE_PATH + "/"
        + uploadDefinition.getFileDefinitions().get(0).getId())
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT)
      .log().all();
  }

  @Test
  public void fileDeleteNotFound() {
    RestAssured.given()
      .spec(spec)
      .when()
      .delete(DEFINITION_PATH + "/"
        + UUID.randomUUID().toString()
        + FILE_PATH + "/"
        + UUID.randomUUID().toString()
      )
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND)
      .log().all();
  }

  @Test
  public void uploadDefinitionDeleteNotFound() {
    RestAssured.given()
      .spec(spec)
      .when()
      .delete(DEFINITION_PATH + "/" + UUID.randomUUID().toString())
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND)
      .log().all();
  }

  @Test
  public void uploadDefinitionDeleteSuccessful() {
    String id = RestAssured.given()
      .spec(spec)
      .body(uploadDef3)
      .when()
      .post(DEFINITION_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .log().all().extract().body().jsonPath().get("id");
    RestAssured.given()
      .spec(spec)
      .when()
      .delete(DEFINITION_PATH + "/" + id)
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT)
      .log().all();
  }

  @Test
  public void uploadDefinitionDeleteBadRequestSuccessfulWhenFailedUpdateJobExecutionStatus() {
    String id = RestAssured.given()
      .spec(spec)
      .body(uploadDef3)
      .when()
      .post(DEFINITION_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .log().all().extract().body().jsonPath().get("id");

    WireMock.stubFor(WireMock.put(new UrlPathPattern(new RegexPattern("/change-manager/jobExecutions/.*/status"), true))
      .willReturn(WireMock.badRequest()));

    RestAssured.given()
      .spec(spec)
      .when()
      .delete(DEFINITION_PATH + "/" + id)
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT)
      .log().all();
  }

  @Test
  public void uploadDefinitionMultipleFilesDeleteSuccessful() {
    String id = RestAssured.given()
      .spec(spec)
      .body(uploadDef4)
      .when()
      .post(DEFINITION_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .log().all().extract().body().jsonPath().get("id");
    RestAssured.given()
      .spec(spec)
      .when()
      .delete(DEFINITION_PATH + "/" + id)
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT)
      .log().all();
  }

  @Test
  public void uploadDefinitionDiscardedFileDeleteSuccessful() {
    UploadDefinition uploadDefinition = RestAssured.given()
      .spec(spec)
      .body(uploadDef4)
      .when()
      .post(DEFINITION_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .log().all().extract().body().as(UploadDefinition.class);
    RestAssured.given()
      .spec(spec)
      .when()
      .delete(DEFINITION_PATH + "/" + uploadDefinition.getId()
        + FILE_PATH + "/" + uploadDefinition.getFileDefinitions().get(0).getId())
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT)
      .log().all();
    RestAssured.given()
      .spec(spec)
      .when()
      .delete(DEFINITION_PATH + "/" + uploadDefinition.getId())
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT)
      .log().all();
  }

  @Test
  public void uploadDefinitionCreateValidationTest() {
    RestAssured.given()
      .spec(spec)
      .body(uploadDef5)
      .when()
      .post(DEFINITION_PATH)
      .then()
      .log().all()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY)
      .body("total_records", is(2));
  }

  @Test
  public void postFilesProcessingSuccessful() {
    UploadDefinition uploadDefinition = new UploadDefinition();
    uploadDefinition.setId(UUID.randomUUID().toString());
    uploadDefinition.setMetaJobExecutionId(UUID.randomUUID().toString());
    uploadDefinition.setCreateDate(new Date());
    uploadDefinition.setStatus(UploadDefinition.Status.IN_PROGRESS);
    JobProfile jobProfile = new JobProfile();
    jobProfile.setId(UUID.randomUUID().toString());
    jobProfile.setName(StringUtils.EMPTY);
    ProcessFilesRqDto processFilesRqDto = new ProcessFilesRqDto()
      .withUploadDefinition(uploadDefinition)
      .withJobProfile(jobProfile);

    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(processFilesRqDto).encode())
      .when()
      .post(DEFINITION_PATH + "/" + uploadDefinition.getId() + PROCESS_FILE_IMPORT_PATH)
      .then()
      .log().all()
      .statusCode(HttpStatus.SC_NO_CONTENT);
  }

  @Test
  public void postFilesProcessingWithUnprocessableEntity() {
    UploadDefinition uploadDefinition = new UploadDefinition()
      .withId(UUID.randomUUID().toString())
      .withStatus(UploadDefinition.Status.IN_PROGRESS);
    JobProfile jobProfile = new JobProfile().withId(UUID.randomUUID().toString());
    ProcessFilesRqDto processFilesRqDto = new ProcessFilesRqDto()
      .withUploadDefinition(uploadDefinition)
      .withJobProfile(jobProfile);

    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(processFilesRqDto).encode())
      .when()
      .post(DEFINITION_PATH + "/" + processFilesRqDto.getUploadDefinition().getId() + PROCESS_FILE_IMPORT_PATH)
      .then()
      .log().all()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
  }
}
