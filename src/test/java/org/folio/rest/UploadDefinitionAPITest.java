package org.folio.rest;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.restassured.RestAssured;
import io.restassured.filter.Filter;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpStatus;
import org.folio.rest.jaxrs.model.FileDefinition;
import org.folio.rest.jaxrs.model.InitJobExecutionsRsDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.ProcessFilesRqDto;
import org.folio.rest.jaxrs.model.UploadDefinition;
import org.folio.service.processing.FileProcessor;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.Objects;
import java.util.UUID;
import lombok.SneakyThrows;

import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static org.folio.dataimport.util.RestUtil.OKAPI_TENANT_HEADER;
import static org.folio.dataimport.util.RestUtil.OKAPI_TOKEN_HEADER;
import static org.folio.dataimport.util.RestUtil.OKAPI_URL_HEADER;
import static org.folio.rest.DefaultFileExtensionAPITest.FILE_EXTENSION_DEFAULT;
import static org.folio.rest.RestVerticle.OKAPI_USERID_HEADER;
import static org.folio.rest.jaxrs.model.UploadDefinition.Status.COMPLETED;
import static org.folio.rest.jaxrs.model.UploadDefinition.Status.ERROR;
import static org.folio.rest.jaxrs.model.UploadDefinition.Status.NEW;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertTrue;

@RunWith(VertxUnitRunner.class)
public class UploadDefinitionAPITest extends AbstractRestTest {

  private static final String DEFINITION_PATH = "/data-import/uploadDefinitions";
  private static final String FILE_PATH = "/files";
  private static final String PROCESS_FILE_IMPORT_PATH = "/processFiles";
  private String uploadDefIdForTest1;
  private String uploadDefIdForTest2;
  private String uploadDefIdForTest3;

  private static final FileDefinition file1 = new FileDefinition()
    .withUiKey("CornellFOLIOExemplars_Bibs(1).mrc.md1547160916680")
    .withName("CornellFOLIOExemplars_Bibs(1).mrc")
    .withSize(209);

  private static final FileDefinition file2 = new FileDefinition()
    .withUiKey("CornellFOLIOExemplars.mrc.md1547160916680")
    .withName("CornellFOLIOExemplars.mrc")
    .withSize(209);

  private static final FileDefinition file3 = new FileDefinition()
    .withUiKey("CornellFOLIOExemplars.mrc.md1547160916680")
    .withName("CornellFOLIOExemplars.mrc")
    .withSize(Integer.MAX_VALUE);

  private static final FileDefinition file4 = new FileDefinition()
    .withUiKey("CornellFOLIOExemplars1.mrc.md1547160916681")
    .withName("CornellFOLIOExemplars1.mrc")
    .withSize(Integer.MAX_VALUE);

  private static final FileDefinition file5 = new FileDefinition()
    .withUiKey("CornellFOLIOExemplars.GIF.md1547160916680")
    .withName("CornellFOLIOExemplars.GIF")
    .withSize(209);

  private static final FileDefinition file6 = new FileDefinition()
    .withUiKey("CornellFOLIOExemplars.jpg.md1547160916680")
    .withName("CornellFOLIOExemplars.jpg")
    .withSize(209);

  private static final UploadDefinition uploadDef1 = new UploadDefinition()
    .withFileDefinitions(Collections.singletonList(file1));

  private static final UploadDefinition uploadDef2 = new UploadDefinition()
    .withFileDefinitions(Collections.singletonList(file1));

  private static final UploadDefinition uploadDef3 = new UploadDefinition()
    .withFileDefinitions(Collections.singletonList(file1));

  private static final UploadDefinition uploadDef4 = new UploadDefinition()
    .withFileDefinitions(Arrays.asList(file1, file2));

  private static final UploadDefinition uploadDef5 = new UploadDefinition()
    .withFileDefinitions(Arrays.asList(file3, file4));

  private static final UploadDefinition uploadDef6 = new UploadDefinition()
    .withFileDefinitions(Collections.singletonList(file5));

  private static final UploadDefinition uploadDef7 = new UploadDefinition()
    .withFileDefinitions(Arrays.asList(file5, file6));

  @After
  public void cleanUpAfterTest() throws IOException {
    FileUtils.deleteDirectory(new File("./storage"));
  }

  @Before
  public void before() {
    uploadDefIdForTest1 = RestAssured.given()
      .spec(spec)
      .body(uploadDef1)
      .when()
      .post(DEFINITION_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .log().all()
      .extract().body().jsonPath().get("id");

    uploadDefIdForTest2 = RestAssured.given()
      .spec(spec)
      .body(uploadDef2)
      .when()
      .post(DEFINITION_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .log().all().extract().body().jsonPath().get("id");

    JobExecution jobExecution = new JobExecution()
      .withId("5105b55a-b9a3-4f76-9402-a5243ea63c97")
      .withParentJobId("5105b55a-b9a3-4f76-9402-a5243ea63c95")
      .withSubordinationType(JobExecution.SubordinationType.PARENT_MULTIPLE)
      .withStatus(JobExecution.Status.NEW)
      .withUiStatus(JobExecution.UiStatus.INITIALIZATION)
      .withUserId(UUID.randomUUID().toString());

    WireMock.stubFor(WireMock.get(new UrlPathPattern(new RegexPattern("/change-manager/jobExecutions/.{36}"), true))
      .willReturn(WireMock.ok().withBody(JsonObject.mapFrom(jobExecution).encode())));

    uploadDefIdForTest3 = RestAssured.given()
      .spec(spec)
      .body(uploadDef1)
      .when()
      .post(DEFINITION_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .log().all().extract().body().jsonPath().get("id");
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
      .body("status", is(NEW.name()))
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
  public void uploadDefinitionGetByUserIdFromToken() {
    String expectedUserId = UUID.randomUUID().toString();
    String token = getUnsecuredJwtWithUserId(expectedUserId);

    Filter requestFilter = (requestSpec, responseSpec, ctx) -> {
      requestSpec.removeHeader(OKAPI_USERID_HEADER);
      return ctx.next(requestSpec, responseSpec);
    };

    String id = RestAssured.given()
      .spec(spec)
      .header(OKAPI_TOKEN_HEADER, token)
      .filter(requestFilter)
      .body(uploadDef1)
      .when()
      .post(DEFINITION_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .log().all().extract().body().jsonPath().get("id");

    RestAssured.given()
      .spec(spec)
      .header(OKAPI_TOKEN_HEADER, token)
      .filter(requestFilter)
      .when()
      .get(DEFINITION_PATH + "?query=status == NEW")
      .then()
      .statusCode(HttpStatus.SC_OK)
      .log().all()
      .body("totalRecords", is(1))
      .body("uploadDefinitions[0].id", is(id))
      .body("uploadDefinitions[0].metadata.createdByUserId", is(expectedUserId));
  }

  @Test
  public void uploadDefinitionGetAllWhenHasNoQuery() {
    Filter requestFilter = (requestSpec, responseSpec, ctx) -> {
      requestSpec.removeHeader(OKAPI_USERID_HEADER);
      return ctx.next(requestSpec, responseSpec);
    };

    RestAssured.given()
      .spec(spec)
      .filter(requestFilter)
      .when()
      .get(DEFINITION_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .log().all()
      .body("totalRecords", is(3));
  }

  @Test
  public void uploadDefinitionGetNotFound() {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(DEFINITION_PATH + "?query=id==" + UUID.randomUUID())
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(0))
      .log().all();
  }

  @Test
  public void uploadDefinitionGetById() {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(DEFINITION_PATH + "/" + uploadDefIdForTest2)
      .then()
      .log().all()
      .statusCode(HttpStatus.SC_OK)
      .body("metaJobExecutionId", notNullValue())
      .body("id", notNullValue())
      .body("status", is(NEW.name()))
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
      .body(uploadDefinition.withMetadata(null))
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
      .put(DEFINITION_PATH + "/" + UUID.randomUUID())
      .then()
      .statusCode(HttpStatus.SC_NOT_FOUND)
      .log().all();
  }

  @Test
  @SneakyThrows
  public void fileUpload() {
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
    String fileId = uploadDefinition.getFileDefinitions().getFirst().getId();
    ClassLoader classLoader = getClass().getClassLoader();
    File file = new File(Objects.requireNonNull(classLoader.getResource("CornellFOLIOExemplars_Bibs.mrc")).toURI());
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
    String path = uploadDefinition1.getFileDefinitions().getFirst().getSourcePath();
    File file2 = new File(path);
    assertTrue(FileUtils.contentEquals(file, file2));
  }

  @Test
  @SneakyThrows
  public void fileUploadFailedIfSrmReturnedException() {
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
    String fileId = uploadDefinition.getFileDefinitions().getFirst().getId();
    String id = uploadDefinition.getFileDefinitions().getFirst().getJobExecutionId();
    WireMock.stubFor(WireMock.put(new UrlPathPattern(new RegexPattern("/change-manager/jobExecutions/" + id + "/status"), true))
      .willReturn(WireMock.notFound()));
    ClassLoader classLoader = getClass().getClassLoader();
    File file = new File(Objects.requireNonNull(classLoader.getResource("CornellFOLIOExemplars_Bibs.mrc")).toURI());
    RestAssured.given()
      .spec(specUpload)
      .when()
      .body(FileUtils.openInputStream(file))
      .post(DEFINITION_PATH + "/" + uploadDefId + FILE_PATH + "/" + fileId)
      .then()
      .log().all()
      .statusCode(HttpStatus.SC_BAD_REQUEST);
  }

  @Test
  @SneakyThrows
  public void fileUploadShouldReturnFileDefinitionWithStatusErrorWhenFileUploadStreamInterrupted() {
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
    String fileId = uploadDefinition.getFileDefinitions().getFirst().getId();

    try (var socket = new Socket("localhost", port);
         var writer = new PrintWriter(socket.getOutputStream())) {
      int falseDataSize = 10;
      writer.print(""
          + "POST " + DEFINITION_PATH + "/" + uploadDefId + FILE_PATH + "/" + fileId + " HTTP/1.0\r\n"
          + "Content-Type: application/octet-stream\r\n"
          + "Accept: application/json,text/plain\r\n"
          + "x-okapi-tenant: " + TENANT_ID + "\r\n"
          + "Content-Length: " + falseDataSize + "\r\n"
          + "\r\n"
          + "123\r\n");
    }

    Awaitility.await().untilAsserted(() -> {
      RestAssured.given()
        .spec(spec)
        .when()
        .get(DEFINITION_PATH + "/" + uploadDefId)
        .then()
        .log().all()
        .statusCode(HttpStatus.SC_OK)
        .body("status", is(ERROR.name()))
        .body("fileDefinitions[0].status", is(FileDefinition.Status.ERROR.name()))
        .body("fileDefinitions.uploadedDate", notNullValue());
    });
  }

  @Test
  @SneakyThrows
  public void fileUploadNotFound() {
    ClassLoader classLoader = getClass().getClassLoader();
    File file = new File(Objects.requireNonNull(classLoader.getResource("CornellFOLIOExemplars_Bibs.mrc")).toURI());
    RestAssured.given()
      .spec(specUpload)
      .when()
      .body(file)
      .post(DEFINITION_PATH + "/"
        + UUID.randomUUID()
        + FILE_PATH + "/"
        + UUID.randomUUID())
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
        + uploadDefinition.getFileDefinitions().getFirst().getId())
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
        + UUID.randomUUID()
        + FILE_PATH + "/"
        + UUID.randomUUID()
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
      .delete(DEFINITION_PATH + "/" + UUID.randomUUID())
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
  public void uploadDefinitionDeleteSuccessfulWithoutStatus() {
    UploadDefinition def = RestAssured.given()
      .spec(spec)
      .body(uploadDef3)
      .when()
      .post(DEFINITION_PATH)
      .then()
      .statusCode(HttpStatus.SC_CREATED)
      .log().all().extract().body().as(UploadDefinition.class);
    String jobId = def.getFileDefinitions().getFirst().getJobExecutionId();
    WireMock.stubFor(WireMock.get(new UrlPathPattern(new RegexPattern("/change-manager/jobExecutions/" + jobId + "?"), true))
      .willReturn(WireMock.badRequest()));
    WireMock.stubFor(WireMock.put(new UrlPathPattern(new RegexPattern("/change-manager/jobExecutions/" + jobId + "status"), true))
      .willReturn(WireMock.badRequest()));

    RestAssured.given()
      .spec(spec)
      .when()
      .delete(DEFINITION_PATH + "/" + def.getId())
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT)
      .log().all();
  }

  @Test
  public void uploadDefinitionDeleteBadRequestWhenFailedUpdateJobExecutionStatus() {
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
  public void uploadDefinitionDeleteBadRequestWhenRelatedJobExecutionsHaveBeingProcessed() {
    JobExecution jobExecution = new JobExecution()
      .withId(UUID.randomUUID().toString())
      .withHrId(1000)
      .withParentJobId(UUID.randomUUID().toString())
      .withSubordinationType(JobExecution.SubordinationType.PARENT_SINGLE)
      .withStatus(JobExecution.Status.PARSING_FINISHED)
      .withUiStatus(JobExecution.UiStatus.RUNNING_COMPLETE)
      .withSourcePath("CornellFOLIOExemplars_Bibs.mrc")
      .withJobProfileInfo(new JobProfileInfo()
        .withName("Marc jobs profile")
        .withId(UUID.randomUUID().toString())
        .withDataType(JobProfileInfo.DataType.MARC))
      .withUserId(UUID.randomUUID().toString());
    WireMock.stubFor(WireMock.get(new UrlPathPattern(new RegexPattern("/change-manager/jobExecutions/.{36}"), true))
      .willReturn(WireMock.ok().withBody(JsonObject.mapFrom(jobExecution).toString())));

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
      .statusCode(HttpStatus.SC_BAD_REQUEST)
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
        + FILE_PATH + "/" + uploadDefinition.getFileDefinitions().getFirst().getId())
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
    // ugly hack to increase coverage for method `process()`

    FileDefinition fileDefinition = new FileDefinition()
      .withName("CornellFOLIOExemplars_Bibs.mrc")
      .withSourcePath("src/test/resources/CornellFOLIOExemplars.mrc")
      .withSize(209);

    String jobExecutionId = UUID.randomUUID().toString();

    UploadDefinition uploadDef = new UploadDefinition();
    uploadDef.setId(UUID.randomUUID().toString());
    uploadDef.setMetaJobExecutionId(jobExecutionId);
    uploadDef.setCreateDate(new Date());
    uploadDef.setStatus(UploadDefinition.Status.IN_PROGRESS);
    uploadDef.setFileDefinitions(Collections.singletonList(fileDefinition));

    JobProfileInfo jobProf = new JobProfileInfo();
    jobProf.setId(UUID.randomUUID().toString());
    jobProf.setName(StringUtils.EMPTY);
    jobProf.setDataType(JobProfileInfo.DataType.MARC);

    ProcessFilesRqDto processFilesReqDto = new ProcessFilesRqDto()
      .withUploadDefinition(uploadDef)
      .withJobProfileInfo(jobProf);

    JsonObject paramsJson = new JsonObject()
      .put(OKAPI_URL_HEADER, OKAPI_URL)
      .put(OKAPI_TENANT_HEADER, TENANT_ID)
      .put(OKAPI_TOKEN_HEADER, TOKEN);

    WireMock.stubFor(WireMock.post(new UrlPathPattern(new RegexPattern("/change-manager/records/.*"), true))
      .willReturn(WireMock.ok()));

    FileProcessor fileProcessor = FileProcessor.create(Vertx.vertx(), null);
    fileProcessor.process(JsonObject.mapFrom(processFilesReqDto), paramsJson);
    UploadDefinition uploadDefinition = new UploadDefinition();
    uploadDefinition.setId(UUID.randomUUID().toString());
    uploadDefinition.setMetaJobExecutionId(UUID.randomUUID().toString());
    uploadDefinition.setCreateDate(new Date());
    uploadDefinition.setStatus(UploadDefinition.Status.IN_PROGRESS);
    JobProfileInfo jobProfile = new JobProfileInfo();
    jobProfile.setId(UUID.randomUUID().toString());
    jobProfile.setName(StringUtils.EMPTY);
    jobProfile.setDataType(JobProfileInfo.DataType.MARC);
    ProcessFilesRqDto processFilesRqDto = new ProcessFilesRqDto()
      .withUploadDefinition(uploadDefinition)
      .withJobProfileInfo(jobProfile);

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
  public void postFilesProcessingUnsuccessful() {
    // ugly hack to increase coverage for method `process()`

    FileDefinition fileDefinition = new FileDefinition()
      .withName("CornellFOLIOExemplars_Bibs.mrc")
      .withSourcePath("src/test/resources/CornellFOLIOExemplars.mrc")
      .withSize(209);

    String jobExecutionId = UUID.randomUUID().toString();

    UploadDefinition uploadDef = new UploadDefinition();
    uploadDef.setId(UUID.randomUUID().toString());
    uploadDef.setMetaJobExecutionId(jobExecutionId);
    uploadDef.setCreateDate(new Date());
    uploadDef.setStatus(UploadDefinition.Status.IN_PROGRESS);
    uploadDef.setFileDefinitions(Collections.singletonList(fileDefinition));

    JobProfileInfo jobProf = new JobProfileInfo();
    jobProf.setId(UUID.randomUUID().toString());
    jobProf.setName(StringUtils.EMPTY);
    jobProf.setDataType(JobProfileInfo.DataType.MARC);

    ProcessFilesRqDto processFilesReqDto = new ProcessFilesRqDto()
      .withUploadDefinition(uploadDef)
      .withJobProfileInfo(jobProf);

    JsonObject paramsJson = new JsonObject()
      .put(OKAPI_URL_HEADER, OKAPI_URL)
      .put(OKAPI_TENANT_HEADER, TENANT_ID)
      .put(OKAPI_TOKEN_HEADER, TOKEN);

    WireMock.stubFor(WireMock.post(new UrlPathPattern(new RegexPattern("/change-manager/records/.*"), true))
      .willReturn(WireMock.serverError()));

    FileProcessor fileProcessor = FileProcessor.create(Vertx.vertx(), null);
    fileProcessor.process(JsonObject.mapFrom(processFilesReqDto), paramsJson);
    UploadDefinition uploadDefinition = new UploadDefinition();
    uploadDefinition.setId(UUID.randomUUID().toString());
    uploadDefinition.setMetaJobExecutionId(UUID.randomUUID().toString());
    uploadDefinition.setCreateDate(new Date());
    uploadDefinition.setStatus(UploadDefinition.Status.IN_PROGRESS);
    JobProfileInfo jobProfile = new JobProfileInfo();
    jobProfile.setId(UUID.randomUUID().toString());
    jobProfile.setName(StringUtils.EMPTY);
    jobProfile.setDataType(JobProfileInfo.DataType.MARC);
    ProcessFilesRqDto processFilesRqDto = new ProcessFilesRqDto()
      .withUploadDefinition(uploadDefinition)
      .withJobProfileInfo(jobProfile);

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
  public void postFilesProcessingSuccessful1() {
    UploadDefinition uploadDefinition = RestAssured.given()
      .spec(spec)
      .body(uploadDef1)
      .when()
      .post(DEFINITION_PATH)
      .then()
      .log().all()
      .statusCode(HttpStatus.SC_CREATED).extract().body().as(UploadDefinition.class);

    JobProfileInfo jobProfile = new JobProfileInfo();
    jobProfile.setId(UUID.randomUUID().toString());
    jobProfile.setName(StringUtils.EMPTY);
    jobProfile.setDataType(JobProfileInfo.DataType.MARC);

    ProcessFilesRqDto request = new ProcessFilesRqDto()
      .withUploadDefinition(uploadDefinition)
      .withJobProfileInfo(jobProfile);

    JobExecution jobExecution = new JobExecution()
      .withId(UUID.randomUUID().toString())
      .withParentJobId(UUID.randomUUID().toString())
      .withSubordinationType(JobExecution.SubordinationType.PARENT_SINGLE);

    WireMock.stubFor(WireMock.post(new UrlPathPattern(new RegexPattern("/change-manager/records/.*"), true))
      .willReturn(WireMock.ok()));
    WireMock.stubFor(WireMock.get(new UrlPathPattern(new RegexPattern("/change-manager/jobExecutions/.*"), true))
      .willReturn(WireMock.ok().withBody(JsonObject.mapFrom(jobExecution).encode())));

    request.getUploadDefinition().withMetadata(null);
    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(request).encode())
      .when()
      .post(DEFINITION_PATH + "/" + uploadDefinition.getId() + PROCESS_FILE_IMPORT_PATH)
      .then()
      .log().all()
      .statusCode(HttpStatus.SC_NO_CONTENT);

    Awaitility.await().untilAsserted(() -> {
      RestAssured.given()
        .spec(spec)
        .when()
        .get(DEFINITION_PATH + "/" + uploadDefinition.getId())
        .then()
        .log().all()
        .statusCode(HttpStatus.SC_OK)
        .body("metaJobExecutionId", notNullValue())
        .body("id", notNullValue())
        .body("status", is(COMPLETED.name()));
    });
  }

  @Test
  public void postFilesProcessingWithUnprocessableEntity() {
    RestAssured.given()
      .spec(spec)
      .body("{}")
      .when()
      .post(DEFINITION_PATH + "/" + UUID.randomUUID().toString() + PROCESS_FILE_IMPORT_PATH)
      .then()
      .log().all()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
  }

  @Test
  public void postFileDefinitionByUploadDefinitionIdCreatedSuccessful() {
    FileDefinition fileDefinition = new FileDefinition()
      .withId("88dfac11-1caf-4470-9ad1-d533f6360bdd")
      .withUploadDefinitionId(uploadDefIdForTest1)
      .withName("marc.mrc");

    RestAssured.given()
      .spec(spec)
      .body(JsonObject.mapFrom(fileDefinition).encode())
      .when()
      .post(DEFINITION_PATH + "/" + fileDefinition.getUploadDefinitionId() + FILE_PATH)
      .then()
      .log().all()
      .statusCode(HttpStatus.SC_CREATED)
      .body("metaJobExecutionId", Matchers.notNullValue())
      .body("id", Matchers.notNullValue())
      .body("status", Matchers.is("NEW"))
      .body("fileDefinitions[0].status", Matchers.is("NEW"))
      .body("fileDefinitions[0].id", Matchers.notNullValue())
      .body("fileDefinitions[1].status", Matchers.is("NEW"))
      .body("fileDefinitions[1].id", Matchers.notNullValue());
  }

  @Test
  public void uploadDefinitionDeleteSuccessfulWhenJobExecutionTypeParentMultiple() {
    RestAssured.given()
      .spec(spec)
      .when()
      .delete(DEFINITION_PATH + "/" + uploadDefIdForTest3)
      .then()
      .statusCode(HttpStatus.SC_NO_CONTENT)
      .log().all();
  }

  @Test
  public void uploadDefinitionCreateBadRequestWhenReceivedJobExecutionWithoutId() {
    JobExecution childrenJobExecution = new JobExecution()
      .withId("55596e0a-cf65-4a10-9c81-58b2c225b03a")
      .withParentJobId("5105b55a-b9a3-4f76-9402-a5243ea63c95")
      .withSourcePath("CornellFOLIOExemplars_Bibs.mrc");

    JobExecution jobExecution = new JobExecution()
      .withParentJobId("")
      .withSubordinationType(JobExecution.SubordinationType.PARENT_SINGLE)
      .withStatus(JobExecution.Status.NEW)
      .withUiStatus(JobExecution.UiStatus.INITIALIZATION)
      .withUserId(UUID.randomUUID().toString());

    InitJobExecutionsRsDto jobExecutionsRespDto = new InitJobExecutionsRsDto()
      .withParentJobExecutionId("5105b55a-b9a3-4f76-9402-a5243ea63c95")
      .withJobExecutions(Arrays.asList(jobExecution, childrenJobExecution));

    WireMock.stubFor(WireMock.post("/change-manager/jobExecutions")
      .willReturn(WireMock.created().withBody(JsonObject.mapFrom(jobExecutionsRespDto).encode())));

    RestAssured.given()
      .spec(spec)
      .body(uploadDef1)
      .when()
      .post(DEFINITION_PATH)
      .then()
      .log().all()
      .statusCode(HttpStatus.SC_BAD_REQUEST);
  }

  @Test
  public void uploadDefinitionCreateBadRequestWhenReceivedChildrenJobExecutionWithoutId() {
    JobExecution childrenJobExecution = new JobExecution()
      .withId("")
      .withParentJobId("5105b55a-b9a3-4f76-9402-a5243ea63c95")
      .withSourcePath("CornellFOLIOExemplars_Bibs.mrc");

    JobExecution jobExecution = new JobExecution()
      .withParentJobId("5105b55a-b9a3-4f76-9402-a5243ea63c95")
      .withSubordinationType(JobExecution.SubordinationType.PARENT_SINGLE)
      .withStatus(JobExecution.Status.NEW)
      .withUiStatus(JobExecution.UiStatus.INITIALIZATION)
      .withUserId(UUID.randomUUID().toString());

    InitJobExecutionsRsDto jobExecutionsRespDto = new InitJobExecutionsRsDto()
      .withParentJobExecutionId("5105b55a-b9a3-4f76-9402-a5243ea63c95")
      .withJobExecutions(Arrays.asList(jobExecution, childrenJobExecution));

    WireMock.stubFor(WireMock.post("/change-manager/jobExecutions")
      .willReturn(WireMock.created().withBody(JsonObject.mapFrom(jobExecutionsRespDto).encode())));

    RestAssured.given()
      .spec(spec)
      .body(uploadDef1)
      .when()
      .post(DEFINITION_PATH)
      .then()
      .log().all()
      .statusCode(HttpStatus.SC_BAD_REQUEST);
  }

  @Test
  public void uploadDefinitionCreateServerErrorWhenFailedJobExecutionsCreation() {
    WireMock.stubFor(WireMock.post("/change-manager/jobExecutions")
      .withRequestBody(matchingJsonPath("$[?(@.files.size() == 1)]"))
      .willReturn(WireMock.serverError()));

    RestAssured.given()
      .spec(spec)
      .body(uploadDef1)
      .when()
      .post(DEFINITION_PATH)
      .then()
      .log().all()
      .statusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
  }

  @Test
  public void uploadDefinitionDeleteServerErrorWhenFailedGettingChildrenJobExecutions() {
    JobExecution jobExecution = new JobExecution()
      .withId("5105b55a-b9a3-4f76-9402-a5243ea63c97")
      .withParentJobId("5105b55a-b9a3-4f76-9402-a5243ea63c95")
      .withSubordinationType(JobExecution.SubordinationType.PARENT_MULTIPLE)
      .withStatus(JobExecution.Status.NEW)
      .withUiStatus(JobExecution.UiStatus.INITIALIZATION)
      .withUserId(UUID.randomUUID().toString());

    WireMock.stubFor(WireMock.get(new UrlPathPattern(new RegexPattern("/change-manager/jobExecutions/.{36}"), true))
      .willReturn(WireMock.ok().withBody(JsonObject.mapFrom(jobExecution).encode())));
    WireMock.stubFor(WireMock.get(new UrlPathPattern(new RegexPattern("/change-manager/jobExecutions/.{36}/children"), true))
      .willReturn(WireMock.serverError()));

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
      .statusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR)
      .log().all();
  }

  @Test
  public void uploadDefinitionDeleteServerErrorWhenFailedGettingJobExecution() {
    WireMock.stubFor(WireMock.get(new UrlPathPattern(new RegexPattern("/change-manager/jobExecutions/.{36}"), true))
      .willReturn(WireMock.serverError()));

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
      .statusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR)
      .log().all();
  }

  @Test
  public void uploadDefinitionDeleteServerErrorWhenFailedMapJobExecutionCollectionFromResponseBody() {
    JsonObject wrongResponseBody = new JsonObject().put("test", "test");
    WireMock.stubFor(WireMock.get(new UrlPathPattern(new RegexPattern("/change-manager/jobExecutions/.{36}/children"), true))
      .willReturn(WireMock.ok().withBody(JsonObject.mapFrom(wrongResponseBody).toString())));

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
      .statusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR)
      .log().all();
  }

  @Test
  public void uploadDefinitionCreateValidateFileExtension() {
    RestAssured.given()
      .spec(spec)
      .when()
      .post(FILE_EXTENSION_DEFAULT)
      .then()
      .log().all()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(13));
    RestAssured.given()
      .spec(spec)
      .body(uploadDef6)
      .when()
      .post(DEFINITION_PATH)
      .then()
      .log().all()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY)
      .body("errors[0].message", is("validation.uploadDefinition.fileExtension.blocked"))
      .body("errors[0].code", is(uploadDef6.getFileDefinitions().getFirst().getName()))
      .body("total_records", is(1));
    RestAssured.given()
      .spec(spec)
      .body(uploadDef7)
      .when()
      .post(DEFINITION_PATH)
      .then()
      .log().all()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY)
      .body("total_records", is(2));
  }

  private String getUnsecuredJwtWithUserId(String userId) {
    String header = new JsonObject().put("alg", "none").encode();
    String payload = new JsonObject()
      .put("user_id", userId)
      .put("tenant", TENANT_ID)
      .encode();

    String encodedHeader = Base64.getEncoder().encodeToString(header.getBytes(StandardCharsets.UTF_8));
    String encodedPayload = Base64.getEncoder().encodeToString(payload.getBytes(StandardCharsets.UTF_8));
    return String.format("%s.%s.", encodedHeader, encodedPayload);
  }
}

