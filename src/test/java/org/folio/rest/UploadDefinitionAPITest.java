package org.folio.rest;

import com.jayway.restassured.RestAssured;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.commons.io.FileUtils;
import org.apache.http.HttpStatus;
import org.drools.core.util.StringUtils;
import org.folio.rest.jaxrs.model.JobProfile;
import org.folio.rest.jaxrs.model.ProcessFilesRqDto;
import org.folio.rest.jaxrs.model.UploadDefinition;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.Objects;
import java.util.UUID;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertTrue;

@RunWith(VertxUnitRunner.class)
public class UploadDefinitionAPITest extends AbstractRestTest {

  private static final String DEFINITION_PATH = "/data-import/uploadDefinitions";
  private static final String FILE_PATH = "/files";
  private static final String PROCESS_FILE_IMPORT_PATH = "/processFiles";

  private static JsonObject file1 = new JsonObject()
    .put("uiKey", "CornellFOLIOExemplars_Bibs.mrc.md1547160916680")
    .put("name", "CornellFOLIOExemplars_Bibs.mrc")
    .put("size", 209);

  private static JsonObject file2 = new JsonObject()
    .put("uiKey", "CornellFOLIOExemplars.mrc.md1547160916680")
    .put("name", "CornellFOLIOExemplars.mrc")
    .put("size", 209);

  private static JsonObject file3 = new JsonObject()
    .put("uiKey", "CornellFOLIOExemplars.mrc.md1547160916680")
    .put("name", "CornellFOLIOExemplars.mrc")
    .put("size", Integer.MAX_VALUE);

  private static JsonObject file4 = new JsonObject()
    .put("uiKey", "CornellFOLIOExemplars1.mrc.md1547160916681")
    .put("name", "CornellFOLIOExemplars1.mrc")
    .put("size", Integer.MAX_VALUE);

  private static JsonObject uploadDef1 = new JsonObject()
    .put("fileDefinitions", new JsonArray().add(file1));

  private static JsonObject uploadDef2 = new JsonObject()
    .put("fileDefinitions", new JsonArray().add(file1));

  private static JsonObject uploadDef3 = new JsonObject()
    .put("fileDefinitions", new JsonArray().add(file1));

  private static JsonObject uploadDef4 = new JsonObject()
    .put("fileDefinitions", new JsonArray().add(file1).add(file2));

  private static JsonObject uploadDef5 = new JsonObject()
    .put("fileDefinitions", new JsonArray().add(file3).add(file4));


  @After
  public void cleanUpAfterTest() throws IOException {
    FileUtils.deleteDirectory(new File("./storage"));
  }

  @Test
  public void uploadDefinitionCreate() {
    RestAssured.given()
      .spec(spec)
      .body(uploadDef1.encode())
      .when()
      .post(DEFINITION_PATH)
      .then()
      .log().all()
      .statusCode(HttpStatus.SC_CREATED)
      .body("metaJobExecutionId", Matchers.notNullValue())
      .body("id", Matchers.notNullValue())
      .body("status", Matchers.is("NEW"))
      .body("fileDefinitions[0].status", Matchers.is("NEW"));
  }

  @Test
  public void uploadDefinitionGet() {
    String id = RestAssured.given()
      .spec(spec)
      .body(uploadDef1.encode())
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
      .body("totalRecords", Matchers.is(1))
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
      .body("totalRecords", Matchers.is(0))
      .log().all();
  }

  @Test
  public void uploadDefinitionGetById() {
    String id = RestAssured.given()
      .spec(spec)
      .body(uploadDef2.encode())
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
      .body("metaJobExecutionId", Matchers.notNullValue())
      .body("id", Matchers.notNullValue())
      .body("status", Matchers.is("NEW"))
      .body("fileDefinitions[0].status", Matchers.is("NEW"));
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
    String object = RestAssured.given()
      .spec(spec)
      .body(uploadDef3.encode())
      .when()
      .post(DEFINITION_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .log().all()
      .extract().body().jsonPath().prettify();
    JsonObject jsonObject = new JsonObject(object);
    jsonObject.put("status", "LOADED");
    RestAssured.given()
      .spec(spec)
      .body(jsonObject.encode())
      .when()
      .put(DEFINITION_PATH + "/" + jsonObject.getString("id"))
      .then()
      .statusCode(HttpStatus.SC_OK)
      .log().all()
      .body("status", Matchers.is("LOADED"));
  }

  @Test
  public void uploadDefinitionUpdateNotFound() {
    RestAssured.given()
      .spec(spec)
      .body(uploadDef3.encode())
      .when()
      .put(DEFINITION_PATH + "/" + UUID.randomUUID().toString())
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND)
      .log().all();
  }

  @Test
  public void fileUpload() throws IOException {
    String object = RestAssured.given()
      .spec(spec)
      .body(uploadDef3.encode())
      .when()
      .post(DEFINITION_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .log().all()
      .extract().body().jsonPath().prettify();
    JsonObject jsonObject = new JsonObject(object);
    String uploadDefId = jsonObject.getString("id");
    String fileId = jsonObject
      .getJsonArray("fileDefinitions")
      .getJsonObject(0)
      .getString("id");

    ClassLoader classLoader = getClass().getClassLoader();
    File file = new File(Objects.requireNonNull(classLoader.getResource("CornellFOLIOExemplars_Bibs.mrc")).getFile());
    String object2 = RestAssured.given()
      .spec(specUpload)
      .when()
      .body(FileUtils.openInputStream(file))
      .post(DEFINITION_PATH + "/" + uploadDefId + FILE_PATH + "/" + fileId)
      .then()
      .log().all()
      .statusCode(HttpStatus.SC_OK)
      .body("status", Matchers.is("LOADED"))
      .body("fileDefinitions[0].status", Matchers.is("UPLOADED"))
      .body("fileDefinitions.uploadedDate", Matchers.notNullValue())
      .extract().body().jsonPath().prettify();
    JsonObject jsonObject2 = new JsonObject(object2);
    String path = jsonObject2
      .getJsonArray("fileDefinitions")
      .getJsonObject(0)
      .getString("sourcePath");
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
    String object = RestAssured.given()
      .spec(spec)
      .body(uploadDef3.encode())
      .when()
      .post(DEFINITION_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .log().all()
      .extract().body().jsonPath().prettify();
    JsonObject jsonObject = new JsonObject(object);
    RestAssured.given()
      .spec(spec)
      .when()
      .delete(DEFINITION_PATH + "/" + jsonObject.getString("id")
        + FILE_PATH + "/"
        + jsonObject.getJsonArray("fileDefinitions")
        .getJsonObject(0)
        .getString("id"))
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
      .body(uploadDef3.encode())
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
  public void uploadDefinitionMultipleFilesDeleteSuccessful() {
    String id = RestAssured.given()
      .spec(spec)
      .body(uploadDef4.encode())
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
      .body(uploadDef4.encode())
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
      .body(uploadDef5.encode())
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
