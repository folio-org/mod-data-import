package org.folio.rest;

import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static org.folio.rest.DefaultFileExtensionRestTest.FILE_EXTENSION_DEFAULT;
import static org.folio.rest.jaxrs.model.UploadDefinition.Status.COMPLETED;
import static org.folio.rest.jaxrs.model.UploadDefinition.Status.ERROR;
import static org.folio.rest.jaxrs.model.UploadDefinition.Status.NEW;
import static org.folio.support.TestUtil.DEFINITION_PATH;
import static org.folio.support.TestUtil.FILE_PATH;
import static org.folio.support.TestUtil.PROCESS_FILE_IMPORT_PATH;
import static org.folio.support.TestUtil.TENANT_ID;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.restassured.filter.Filter;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
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
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.jaxrs.model.FileDefinition;
import org.folio.rest.jaxrs.model.InitJobExecutionsRsDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.ProcessFilesRqDto;
import org.folio.rest.jaxrs.model.UploadDefinition;
import org.folio.service.processing.FileProcessor;
import org.folio.support.AbstractRestTest;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.testcontainers.shaded.org.awaitility.Awaitility;

class UploadDefinitionRestTest extends AbstractRestTest {

  private static final FileDefinition FILE_1 = new FileDefinition()
    .withUiKey("CornellFOLIOExemplars_Bibs(1).mrc.md1547160916680")
    .withName("CornellFOLIOExemplars_Bibs(1).mrc").withSize(209);
  private static final FileDefinition FILE_2 = new FileDefinition()
    .withUiKey("CornellFOLIOExemplars.mrc.md1547160916680")
    .withName("CornellFOLIOExemplars.mrc").withSize(209);
  private static final FileDefinition FILE_3 = new FileDefinition()
    .withUiKey("CornellFOLIOExemplars.mrc.md1547160916680")
    .withName("CornellFOLIOExemplars.mrc").withSize(Integer.MAX_VALUE);
  private static final FileDefinition FILE_4 = new FileDefinition()
    .withUiKey("CornellFOLIOExemplars1.mrc.md1547160916681")
    .withName("CornellFOLIOExemplars1.mrc").withSize(Integer.MAX_VALUE);
  private static final FileDefinition FILE_5 = new FileDefinition()
    .withUiKey("CornellFOLIOExemplars.GIF.md1547160916680")
    .withName("CornellFOLIOExemplars.GIF").withSize(209);
  private static final FileDefinition FILE_6 = new FileDefinition()
    .withUiKey("CornellFOLIOExemplars.jpg.md1547160916680")
    .withName("CornellFOLIOExemplars.jpg").withSize(209);
  private static final UploadDefinition UPLOAD_DEF_1 = new UploadDefinition()
    .withFileDefinitions(Collections.singletonList(FILE_1));
  private static final UploadDefinition UPLOAD_DEF_2 = new UploadDefinition()
    .withFileDefinitions(Collections.singletonList(FILE_1));
  private static final UploadDefinition UPLOAD_DEF_3 = new UploadDefinition()
    .withFileDefinitions(Collections.singletonList(FILE_1));
  private static final UploadDefinition UPLOAD_DEF_4 = new UploadDefinition()
    .withFileDefinitions(Arrays.asList(FILE_1, FILE_2));
  private static final UploadDefinition UPLOAD_DEF_5 = new UploadDefinition()
    .withFileDefinitions(Arrays.asList(FILE_3, FILE_4));
  private static final UploadDefinition UPLOAD_DEF_6 = new UploadDefinition()
    .withFileDefinitions(Collections.singletonList(FILE_5));
  private static final UploadDefinition UPLOAD_DEF_7 = new UploadDefinition()
    .withFileDefinitions(Arrays.asList(FILE_5, FILE_6));
  private String uploadDefIdForTest1;
  private String uploadDefIdForTest2;
  private String uploadDefIdForTest3;

  @AfterEach
  void cleanUpAfterTest() throws IOException {
    FileUtils.deleteDirectory(new File("./storage"));
  }

  @BeforeEach
  void before() {
    uploadDefIdForTest1 = postRequest(DEFINITION_PATH, UPLOAD_DEF_1)
      .statusCode(HttpStatus.SC_CREATED).log().all().extract().body().jsonPath().get("id");

    uploadDefIdForTest2 = postRequest(DEFINITION_PATH, UPLOAD_DEF_2)
      .statusCode(HttpStatus.SC_CREATED).log().all().extract().body().jsonPath().get("id");

    JobExecution jobExecution = new JobExecution()
      .withId("5105b55a-b9a3-4f76-9402-a5243ea63c97")
      .withParentJobId("5105b55a-b9a3-4f76-9402-a5243ea63c95")
      .withSubordinationType(JobExecution.SubordinationType.PARENT_MULTIPLE)
      .withStatus(JobExecution.Status.NEW)
      .withUiStatus(JobExecution.UiStatus.INITIALIZATION)
      .withUserId(UUID.randomUUID().toString());

    WIRE_MOCK.stubFor(WireMock.get(new UrlPathPattern(new RegexPattern("/change-manager/jobExecutions/.{36}"), true))
      .willReturn(WireMock.ok().withBody(JsonObject.mapFrom(jobExecution).encode())));

    uploadDefIdForTest3 = postRequest(DEFINITION_PATH, UPLOAD_DEF_1)
      .statusCode(HttpStatus.SC_CREATED).log().all().extract().body().jsonPath().get("id");
  }

  @DisplayName("should create upload definition and return 201 with status NEW")
  @Test
  void shouldCreateUploadDefinition() {
    postRequest(DEFINITION_PATH, UPLOAD_DEF_1)
      .log().all()
      .statusCode(HttpStatus.SC_CREATED)
      .body("metaJobExecutionId", notNullValue())
      .body("id", notNullValue())
      .body("status", is(NEW.name()))
      .body("fileDefinitions[0].status", is(FileDefinition.Status.NEW.name()));
  }

  @DisplayName("should return upload definition matching query")
  @Test
  void shouldReturnUploadDefinition_whenQueryById() {
    String id = postRequest(DEFINITION_PATH, UPLOAD_DEF_1)
      .statusCode(HttpStatus.SC_CREATED).log().all().extract().body().jsonPath().get("id");

    getRequest(DEFINITION_PATH + "?query=id==" + id)
      .statusCode(HttpStatus.SC_OK).body("totalRecords", is(1)).log().all();
  }

  @DisplayName("should return upload definition with userId extracted from token")
  @Test
  void shouldReturnUploadDefinition_whenUserIdExtractedFromToken() {
    String expectedUserId = UUID.randomUUID().toString();
    String token = getUnsecuredJwtWithUserId(expectedUserId);

    Filter requestFilter = (requestSpec, responseSpec, ctx) -> {
      requestSpec.removeHeader(XOkapiHeaders.USER_ID);
      return ctx.next(requestSpec, responseSpec);
    };

    String id = given()
      .header(XOkapiHeaders.TOKEN, token).filter(requestFilter).body(JsonObject.mapFrom(UPLOAD_DEF_1).encode())
      .post(DEFINITION_PATH).then().statusCode(HttpStatus.SC_CREATED).log().all().extract().body().jsonPath().get("id");

    given()
      .header(XOkapiHeaders.TOKEN, token).filter(requestFilter)
      .get(DEFINITION_PATH + "?query=status == NEW")
      .then().statusCode(HttpStatus.SC_OK).log().all()
      .body("totalRecords", is(1))
      .body("uploadDefinitions[0].id", is(id))
      .body("uploadDefinitions[0].metadata.createdByUserId", is(expectedUserId));
  }

  @DisplayName("should return all upload definitions when no query is specified")
  @Test
  void shouldReturnAllUploadDefinitions_whenNoQuery() {
    Filter requestFilter = (requestSpec, responseSpec, ctx) -> {
      requestSpec.removeHeader(XOkapiHeaders.USER_ID);
      return ctx.next(requestSpec, responseSpec);
    };

    given().filter(requestFilter)
      .get(DEFINITION_PATH)
      .then().statusCode(HttpStatus.SC_OK).log().all()
      .body("totalRecords", is(3));
  }

  @DisplayName("should return 0 records when no upload definition matches query")
  @Test
  void shouldReturnZeroRecords_whenNoUploadDefinitionMatchesQuery() {
    getRequest(DEFINITION_PATH + "?query=id==" + UUID.randomUUID())
      .statusCode(HttpStatus.SC_OK).body("totalRecords", is(0)).log().all();
  }

  @DisplayName("should return upload definition by ID")
  @Test
  void shouldReturnUploadDefinitionById() {
    getRequest(DEFINITION_PATH + "/" + uploadDefIdForTest2)
      .log().all()
      .statusCode(HttpStatus.SC_OK)
      .body("metaJobExecutionId", notNullValue())
      .body("id", notNullValue())
      .body("status", is(NEW.name()))
      .body("fileDefinitions[0].status", is(FileDefinition.Status.NEW.name()));
  }

  @DisplayName("should return 404 when upload definition not found by ID")
  @Test
  void shouldReturn404_whenUploadDefinitionNotFoundById() {
    getRequest(DEFINITION_PATH + "/" + UUID.randomUUID())
      .statusCode(HttpStatus.SC_NOT_FOUND).log().all();
  }

  @DisplayName("should update upload definition status")
  @Test
  void shouldUpdateUploadDefinitionStatus() {
    UploadDefinition uploadDefinition = postRequest(DEFINITION_PATH, UPLOAD_DEF_3)
      .statusCode(HttpStatus.SC_CREATED).log().all().extract().body().as(UploadDefinition.class);

    uploadDefinition.setStatus(UploadDefinition.Status.LOADED);

    putRequest(DEFINITION_PATH + "/" + uploadDefinition.getId(), uploadDefinition.withMetadata(null))
      .statusCode(HttpStatus.SC_OK).log().all()
      .body("status", is(UploadDefinition.Status.LOADED.name()));
  }

  @DisplayName("should return 404 when updating non-existent upload definition")
  @Test
  void shouldReturn404_whenUpdatingNonExistentUploadDefinition() {
    putRequest(DEFINITION_PATH + "/" + UUID.randomUUID(), UPLOAD_DEF_3)
      .statusCode(HttpStatus.SC_NOT_FOUND).log().all();
  }

  @DisplayName("should upload file successfully and return LOADED status")
  @Test
  @SneakyThrows
  void shouldUploadFile_andReturnLoadedStatus() {
    UploadDefinition uploadDefinition = postRequest(DEFINITION_PATH, UPLOAD_DEF_3)
      .statusCode(HttpStatus.SC_CREATED).log().all().extract().body().as(UploadDefinition.class);

    String uploadDefId = uploadDefinition.getId();
    String fileId = uploadDefinition.getFileDefinitions().getFirst().getId();
    File file = new File(Objects.requireNonNull(
      getClass().getClassLoader().getResource("CornellFOLIOExemplars_Bibs.mrc")).toURI());

    UploadDefinition uploadDefinition1 = given().spec(specUpload)
      .body(FileUtils.openInputStream(file))
      .post(DEFINITION_PATH + "/" + uploadDefId + FILE_PATH + "/" + fileId)
      .then().log().all()
      .statusCode(HttpStatus.SC_OK)
      .body("status", is(UploadDefinition.Status.LOADED.name()))
      .body("fileDefinitions[0].status", is(FileDefinition.Status.UPLOADED.name()))
      .body("fileDefinitions.uploadedDate", notNullValue())
      .extract().body().as(UploadDefinition.class);

    String path = uploadDefinition1.getFileDefinitions().getFirst().getSourcePath();
    assertTrue(FileUtils.contentEquals(file, new File(path)));
  }

  @DisplayName("should return 400 when SRM returns an exception during file upload")
  @Test
  @SneakyThrows
  void shouldReturn400_whenSrmReturnedException() {
    UploadDefinition uploadDefinition = postRequest(DEFINITION_PATH, UPLOAD_DEF_3)
      .statusCode(HttpStatus.SC_CREATED).log().all().extract().body().as(UploadDefinition.class);

    String uploadDefId = uploadDefinition.getId();
    String fileId = uploadDefinition.getFileDefinitions().getFirst().getId();
    String id = uploadDefinition.getFileDefinitions().getFirst().getJobExecutionId();

    WIRE_MOCK.stubFor(WireMock.put(new UrlPathPattern(
        new RegexPattern("/change-manager/jobExecutions/" + id + "/status"), true))
      .willReturn(WireMock.notFound()));

    File file = new File(Objects.requireNonNull(
      getClass().getClassLoader().getResource("CornellFOLIOExemplars_Bibs.mrc")).toURI());

    given().spec(specUpload).body(FileUtils.openInputStream(file))
      .post(DEFINITION_PATH + "/" + uploadDefId + FILE_PATH + "/" + fileId)
      .then().log().all()
      .statusCode(HttpStatus.SC_BAD_REQUEST);
  }

  @DisplayName("should return ERROR status when file upload stream is interrupted")
  @Test
  @SneakyThrows
  void shouldReturnErrorStatus_whenFileUploadStreamInterrupted() {
    UploadDefinition uploadDefinition = postRequest(DEFINITION_PATH, UPLOAD_DEF_3)
      .statusCode(HttpStatus.SC_CREATED).log().all().extract().body().as(UploadDefinition.class);

    var uploadDefId = getUploadDefId(uploadDefinition);

    Awaitility.await().untilAsserted(() ->
      getRequest(DEFINITION_PATH + "/" + uploadDefId)
        .log().all()
        .statusCode(HttpStatus.SC_OK)
        .body("status", is(ERROR.name()))
        .body("fileDefinitions[0].status", is(FileDefinition.Status.ERROR.name()))
        .body("fileDefinitions.uploadedDate", notNullValue()));
  }

  @DisplayName("should return 404 when uploading file for non-existent upload definition")
  @Test
  @SneakyThrows
  void shouldReturn404_whenUploadDefinitionNotFoundForFileUpload() {
    File file = new File(Objects.requireNonNull(
      getClass().getClassLoader().getResource("CornellFOLIOExemplars_Bibs.mrc")).toURI());

    given().spec(specUpload).body(file)
      .post(DEFINITION_PATH + "/" + UUID.randomUUID() + FILE_PATH + "/" + UUID.randomUUID())
      .then().log().all()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @DisplayName("should delete file definition successfully")
  @Test
  void shouldDeleteFileDefinition() {
    UploadDefinition uploadDefinition = postRequest(DEFINITION_PATH, UPLOAD_DEF_3)
      .statusCode(HttpStatus.SC_CREATED).log().all().extract().body().as(UploadDefinition.class);

    deleteRequest(DEFINITION_PATH + "/" + uploadDefinition.getId()
                  + FILE_PATH + "/" + uploadDefinition.getFileDefinitions().getFirst().getId())
      .statusCode(HttpStatus.SC_NO_CONTENT).log().all();
  }

  @DisplayName("should return 404 when deleting non-existent file definition")
  @Test
  void shouldReturn404_whenDeletingNonExistentFileDefinition() {
    deleteRequest(DEFINITION_PATH + "/" + UUID.randomUUID() + FILE_PATH + "/" + UUID.randomUUID())
      .statusCode(HttpStatus.SC_NOT_FOUND).log().all();
  }

  @DisplayName("should return 404 when deleting non-existent upload definition")
  @Test
  void shouldReturn404_whenDeletingNonExistentUploadDefinition() {
    deleteRequest(DEFINITION_PATH + "/" + UUID.randomUUID())
      .statusCode(HttpStatus.SC_NOT_FOUND).log().all();
  }

  @DisplayName("should delete upload definition successfully")
  @Test
  void shouldDeleteUploadDefinition_successfully() {
    String id = postRequest(DEFINITION_PATH, UPLOAD_DEF_3)
      .statusCode(HttpStatus.SC_CREATED).log().all().extract().body().jsonPath().get("id");

    deleteRequest(DEFINITION_PATH + "/" + id)
      .statusCode(HttpStatus.SC_NO_CONTENT).log().all();
  }

  @DisplayName("should delete upload definition even when job execution status update fails")
  @Test
  void shouldDeleteUploadDefinition_whenJobExecutionStatusUpdateFails() {
    UploadDefinition def = postRequest(DEFINITION_PATH, UPLOAD_DEF_3)
      .statusCode(HttpStatus.SC_CREATED).log().all().extract().body().as(UploadDefinition.class);

    String jobId = def.getFileDefinitions().getFirst().getJobExecutionId();
    WIRE_MOCK.stubFor(
      WireMock.get(new UrlPathPattern(new RegexPattern("/change-manager/jobExecutions/" + jobId + "?"), true))
        .willReturn(WireMock.badRequest()));
    WIRE_MOCK.stubFor(
      WireMock.put(new UrlPathPattern(new RegexPattern("/change-manager/jobExecutions/" + jobId + "status"), true))
        .willReturn(WireMock.badRequest()));

    deleteRequest(DEFINITION_PATH + "/" + def.getId())
      .statusCode(HttpStatus.SC_NO_CONTENT).log().all();
  }

  @DisplayName("should return 204 when deleting upload definition with bad request on job execution status update")
  @Test
  void shouldReturn204_whenJobExecutionStatusUpdateBadRequest() {
    String id = postRequest(DEFINITION_PATH, UPLOAD_DEF_3)
      .statusCode(HttpStatus.SC_CREATED).log().all().extract().body().jsonPath().get("id");

    WIRE_MOCK.stubFor(
      WireMock.put(new UrlPathPattern(new RegexPattern("/change-manager/jobExecutions/.*/status"), true))
        .willReturn(WireMock.badRequest()));

    deleteRequest(DEFINITION_PATH + "/" + id)
      .statusCode(HttpStatus.SC_NO_CONTENT).log().all();
  }

  @DisplayName("should return 400 when deleting upload definition with processing job executions")
  @Test
  void shouldReturn400_whenRelatedJobExecutionsAreBeingProcessed() {
    JobExecution jobExecution = new JobExecution()
      .withId(UUID.randomUUID().toString()).withHrId(1000)
      .withParentJobId(UUID.randomUUID().toString())
      .withSubordinationType(JobExecution.SubordinationType.PARENT_SINGLE)
      .withStatus(JobExecution.Status.PARSING_FINISHED)
      .withUiStatus(JobExecution.UiStatus.RUNNING_COMPLETE)
      .withSourcePath("CornellFOLIOExemplars_Bibs.mrc")
      .withJobProfileInfo(new JobProfileInfo().withName("Marc jobs profile")
        .withId(UUID.randomUUID().toString()).withDataType(JobProfileInfo.DataType.MARC))
      .withUserId(UUID.randomUUID().toString());

    WIRE_MOCK.stubFor(WireMock.get(new UrlPathPattern(new RegexPattern("/change-manager/jobExecutions/.{36}"), true))
      .willReturn(WireMock.ok().withBody(JsonObject.mapFrom(jobExecution).toString())));

    String id = postRequest(DEFINITION_PATH, UPLOAD_DEF_3)
      .statusCode(HttpStatus.SC_CREATED).log().all().extract().body().jsonPath().get("id");

    deleteRequest(DEFINITION_PATH + "/" + id)
      .statusCode(HttpStatus.SC_BAD_REQUEST).log().all();
  }

  @DisplayName("should delete upload definition with multiple files successfully")
  @Test
  void shouldDeleteUploadDefinitionWithMultipleFiles() {
    String id = postRequest(DEFINITION_PATH, UPLOAD_DEF_4)
      .statusCode(HttpStatus.SC_CREATED).log().all().extract().body().jsonPath().get("id");

    deleteRequest(DEFINITION_PATH + "/" + id)
      .statusCode(HttpStatus.SC_NO_CONTENT).log().all();
  }

  @DisplayName("should delete upload definition when a file has been discarded")
  @Test
  void shouldDeleteUploadDefinition_whenFileIsDiscarded() {
    UploadDefinition uploadDefinition = postRequest(DEFINITION_PATH, UPLOAD_DEF_4)
      .statusCode(HttpStatus.SC_CREATED).log().all().extract().body().as(UploadDefinition.class);

    deleteRequest(DEFINITION_PATH + "/" + uploadDefinition.getId()
                  + FILE_PATH + "/" + uploadDefinition.getFileDefinitions().getFirst().getId())
      .statusCode(HttpStatus.SC_NO_CONTENT).log().all();

    deleteRequest(DEFINITION_PATH + "/" + uploadDefinition.getId())
      .statusCode(HttpStatus.SC_NO_CONTENT).log().all();
  }

  @DisplayName("should return 422 when upload definition has validation errors")
  @Test
  void shouldReturn422_whenUploadDefinitionHasValidationErrors() {
    postRequest(DEFINITION_PATH, UPLOAD_DEF_5)
      .log().all()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY)
      .body("total_records", is(2));
  }

  @DisplayName("should return 204 when processing files successfully")
  @Test
  void shouldReturn204_whenProcessingFilesSuccessfully() {
    FileDefinition fileDefinition = new FileDefinition()
      .withName("CornellFOLIOExemplars_Bibs.mrc")
      .withSourcePath("src/test/resources/CornellFOLIOExemplars.mrc").withSize(209);

    UploadDefinition uploadDef = new UploadDefinition()
      .withId(UUID.randomUUID().toString())
      .withMetaJobExecutionId(UUID.randomUUID().toString())
      .withCreateDate(new Date())
      .withStatus(UploadDefinition.Status.IN_PROGRESS)
      .withFileDefinitions(Collections.singletonList(fileDefinition));

    JsonObject paramsJson = new JsonObject()
      .put(XOkapiHeaders.URL, mockServerUrl())
      .put(XOkapiHeaders.TENANT, TENANT_ID)
      .put(XOkapiHeaders.TOKEN, TOKEN);

    FileProcessor fileProcessor = FileProcessor.create(Vertx.vertx(), null);
    fileProcessor.process(JsonObject.mapFrom(new ProcessFilesRqDto()
      .withUploadDefinition(uploadDef)
      .withJobProfileInfo(new JobProfileInfo().withId(UUID.randomUUID().toString())
        .withName(StringUtils.EMPTY).withDataType(JobProfileInfo.DataType.MARC))), paramsJson);

    UploadDefinition uploadDefinition = postRequest(DEFINITION_PATH, UPLOAD_DEF_1)
      .log().all().statusCode(HttpStatus.SC_CREATED).extract().body().as(UploadDefinition.class);

    ProcessFilesRqDto processFilesRqDto = new ProcessFilesRqDto()
      .withUploadDefinition(uploadDefinition)
      .withJobProfileInfo(new JobProfileInfo().withId(UUID.randomUUID().toString())
        .withName(StringUtils.EMPTY).withDataType(JobProfileInfo.DataType.MARC));

    postRequest(DEFINITION_PATH + "/" + uploadDefinition.getId() + PROCESS_FILE_IMPORT_PATH,
      JsonObject.mapFrom(processFilesRqDto).encode())
      .log().all()
      .statusCode(HttpStatus.SC_NO_CONTENT);

    Awaitility.await().untilAsserted(() ->
      getRequest(DEFINITION_PATH + "/" + uploadDefinition.getId())
        .log().all().statusCode(HttpStatus.SC_OK)
        .body("metaJobExecutionId", is(uploadDefinition.getMetaJobExecutionId()))
        .body("id", notNullValue())
        .body("status", is(COMPLETED.name())));
  }

  @DisplayName("should return 404 when processing files with non-existent upload definition")
  @Test
  void shouldReturn404_whenProcessingFilesWithNonExistentUploadDefinition() {
    UploadDefinition uploadDef = new UploadDefinition()
      .withId(UUID.randomUUID().toString())
      .withMetaJobExecutionId(UUID.randomUUID().toString())
      .withCreateDate(new Date())
      .withStatus(UploadDefinition.Status.IN_PROGRESS);

    JsonObject paramsJson = new JsonObject()
      .put(XOkapiHeaders.URL, mockServerUrl())
      .put(XOkapiHeaders.TENANT, TENANT_ID)
      .put(XOkapiHeaders.TOKEN, TOKEN);

    WIRE_MOCK.stubFor(WireMock.post(new UrlPathPattern(new RegexPattern("/change-manager/records/.*"), true))
      .willReturn(WireMock.serverError()));

    FileProcessor.create(Vertx.vertx(), null)
      .process(JsonObject.mapFrom(new ProcessFilesRqDto()
        .withUploadDefinition(uploadDef)
        .withJobProfileInfo(new JobProfileInfo().withId(UUID.randomUUID().toString())
          .withName(StringUtils.EMPTY).withDataType(JobProfileInfo.DataType.MARC))), paramsJson);

    UploadDefinition uploadDefinition = new UploadDefinition()
      .withId(UUID.randomUUID().toString())
      .withMetaJobExecutionId(UUID.randomUUID().toString())
      .withCreateDate(new Date())
      .withStatus(UploadDefinition.Status.IN_PROGRESS);

    postRequest(DEFINITION_PATH + "/" + uploadDefinition.getId() + PROCESS_FILE_IMPORT_PATH,
      JsonObject.mapFrom(new ProcessFilesRqDto()
        .withUploadDefinition(uploadDefinition)
        .withJobProfileInfo(new JobProfileInfo().withId(UUID.randomUUID().toString())
          .withName(StringUtils.EMPTY).withDataType(JobProfileInfo.DataType.MARC))).encode())
      .log().all()
      .statusCode(HttpStatus.SC_NOT_FOUND);
  }

  @DisplayName("should return 204 when processing files with PARENT_SINGLE job execution")
  @Test
  void shouldReturn204_whenProcessingFilesWithParentSingleExecution() {
    UploadDefinition uploadDefinition = postRequest(DEFINITION_PATH, UPLOAD_DEF_1)
      .log().all().statusCode(HttpStatus.SC_CREATED).extract().body().as(UploadDefinition.class);

    JobExecution jobExecution = new JobExecution()
      .withId(UUID.randomUUID().toString()).withParentJobId(UUID.randomUUID().toString())
      .withSubordinationType(JobExecution.SubordinationType.PARENT_SINGLE);

    WIRE_MOCK.stubFor(WireMock.post(new UrlPathPattern(new RegexPattern("/change-manager/records/.*"), true))
      .willReturn(WireMock.ok()));
    WIRE_MOCK.stubFor(WireMock.get(new UrlPathPattern(new RegexPattern("/change-manager/jobExecutions/.*"), true))
      .willReturn(WireMock.ok().withBody(JsonObject.mapFrom(jobExecution).encode())));

    ProcessFilesRqDto request = new ProcessFilesRqDto()
      .withUploadDefinition(uploadDefinition.withMetadata(null))
      .withJobProfileInfo(new JobProfileInfo().withId(UUID.randomUUID().toString())
        .withName(StringUtils.EMPTY).withDataType(JobProfileInfo.DataType.MARC));

    postRequest(DEFINITION_PATH + "/" + uploadDefinition.getId() + PROCESS_FILE_IMPORT_PATH,
      JsonObject.mapFrom(request).encode())
      .log().all()
      .statusCode(HttpStatus.SC_NO_CONTENT);

    Awaitility.await().untilAsserted(() ->
      getRequest(DEFINITION_PATH + "/" + uploadDefinition.getId())
        .log().all().statusCode(HttpStatus.SC_OK)
        .body("metaJobExecutionId", notNullValue())
        .body("id", notNullValue())
        .body("status", is(COMPLETED.name())));
  }

  @DisplayName("should return 422 when processing files with empty body")
  @Test
  void shouldReturn422_whenProcessingFilesWithEmptyBody() {
    postRequest(DEFINITION_PATH + "/" + UUID.randomUUID() + PROCESS_FILE_IMPORT_PATH, "{}")
      .log().all()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
  }

  @DisplayName("should add file definition to upload definition successfully")
  @Test
  void shouldAddFileDefinitionToUploadDefinition() {
    FileDefinition fileDefinition = new FileDefinition()
      .withId("88dfac11-1caf-4470-9ad1-d533f6360bdd")
      .withUploadDefinitionId(uploadDefIdForTest1)
      .withName("marc.mrc");

    postRequest(DEFINITION_PATH + "/" + fileDefinition.getUploadDefinitionId() + FILE_PATH,
      JsonObject.mapFrom(fileDefinition).encode())
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

  @DisplayName("should delete upload definition with PARENT_MULTIPLE job execution type")
  @Test
  void shouldDeleteUploadDefinition_whenJobExecutionTypeIsParentMultiple() {
    deleteRequest(DEFINITION_PATH + "/" + uploadDefIdForTest3)
      .statusCode(HttpStatus.SC_NO_CONTENT).log().all();
  }

  @DisplayName("should return 400 when received job execution without ID")
  @Test
  void shouldReturn400_whenJobExecutionHasNoId() {
    JobExecution jobExecution = new JobExecution()
      .withParentJobId("").withSubordinationType(JobExecution.SubordinationType.PARENT_SINGLE)
      .withStatus(JobExecution.Status.NEW).withUiStatus(JobExecution.UiStatus.INITIALIZATION)
      .withUserId(UUID.randomUUID().toString());

    InitJobExecutionsRsDto jobExecutionsRespDto = new InitJobExecutionsRsDto()
      .withParentJobExecutionId("5105b55a-b9a3-4f76-9402-a5243ea63c95")
      .withJobExecutions(Arrays.asList(jobExecution,
        new JobExecution().withId("55596e0a-cf65-4a10-9c81-58b2c225b03a")
          .withParentJobId("5105b55a-b9a3-4f76-9402-a5243ea63c95")
          .withSourcePath("CornellFOLIOExemplars_Bibs.mrc")));

    WIRE_MOCK.stubFor(WireMock.post("/change-manager/jobExecutions")
      .willReturn(WireMock.created().withBody(JsonObject.mapFrom(jobExecutionsRespDto).encode())));

    postRequest(DEFINITION_PATH, UPLOAD_DEF_1).log().all().statusCode(HttpStatus.SC_BAD_REQUEST);
  }

  @DisplayName("should return 400 when received children job execution without ID")
  @Test
  void shouldReturn400_whenChildrenJobExecutionHasNoId() {
    InitJobExecutionsRsDto jobExecutionsRespDto = new InitJobExecutionsRsDto()
      .withParentJobExecutionId("5105b55a-b9a3-4f76-9402-a5243ea63c95")
      .withJobExecutions(Arrays.asList(
        new JobExecution().withParentJobId("5105b55a-b9a3-4f76-9402-a5243ea63c95")
          .withSubordinationType(JobExecution.SubordinationType.PARENT_SINGLE)
          .withStatus(JobExecution.Status.NEW).withUiStatus(JobExecution.UiStatus.INITIALIZATION)
          .withUserId(UUID.randomUUID().toString()),
        new JobExecution().withId("").withParentJobId("5105b55a-b9a3-4f76-9402-a5243ea63c95")
          .withSourcePath("CornellFOLIOExemplars_Bibs.mrc")));

    WIRE_MOCK.stubFor(WireMock.post("/change-manager/jobExecutions")
      .willReturn(WireMock.created().withBody(JsonObject.mapFrom(jobExecutionsRespDto).encode())));

    postRequest(DEFINITION_PATH, UPLOAD_DEF_1).log().all().statusCode(HttpStatus.SC_BAD_REQUEST);
  }

  @DisplayName("should return 500 when job executions creation fails")
  @Test
  void shouldReturn500_whenJobExecutionsCreationFails() {
    WIRE_MOCK.stubFor(WireMock.post("/change-manager/jobExecutions")
      .withRequestBody(matchingJsonPath("$[?(@.files.size() == 1)]"))
      .willReturn(WireMock.serverError()));

    postRequest(DEFINITION_PATH, UPLOAD_DEF_1).log().all().statusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
  }

  @DisplayName("should return 500 when getting children job executions fails")
  @Test
  void shouldReturn500_whenGettingChildrenJobExecutionsFails() {
    JobExecution jobExecution = new JobExecution()
      .withId("5105b55a-b9a3-4f76-9402-a5243ea63c97")
      .withParentJobId("5105b55a-b9a3-4f76-9402-a5243ea63c95")
      .withSubordinationType(JobExecution.SubordinationType.PARENT_MULTIPLE)
      .withStatus(JobExecution.Status.NEW).withUiStatus(JobExecution.UiStatus.INITIALIZATION)
      .withUserId(UUID.randomUUID().toString());

    WIRE_MOCK.stubFor(WireMock.get(new UrlPathPattern(new RegexPattern("/change-manager/jobExecutions/.{36}"), true))
      .willReturn(WireMock.ok().withBody(JsonObject.mapFrom(jobExecution).encode())));
    WIRE_MOCK.stubFor(
      WireMock.get(new UrlPathPattern(new RegexPattern("/change-manager/jobExecutions/.{36}/children"), true))
        .willReturn(WireMock.serverError()));

    String id = postRequest(DEFINITION_PATH, UPLOAD_DEF_3)
      .statusCode(HttpStatus.SC_CREATED).log().all().extract().body().jsonPath().get("id");

    deleteRequest(DEFINITION_PATH + "/" + id)
      .statusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR).log().all();
  }

  @DisplayName("should return 500 when getting job execution fails")
  @Test
  void shouldReturn500_whenGettingJobExecutionFails() {
    WIRE_MOCK.stubFor(WireMock.get(new UrlPathPattern(new RegexPattern("/change-manager/jobExecutions/.{36}"), true))
      .willReturn(WireMock.serverError()));

    String id = postRequest(DEFINITION_PATH, UPLOAD_DEF_3)
      .statusCode(HttpStatus.SC_CREATED).log().all().extract().body().jsonPath().get("id");

    deleteRequest(DEFINITION_PATH + "/" + id)
      .statusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR).log().all();
  }

  @DisplayName("should return 500 when mapping job execution collection from response body fails")
  @Test
  void shouldReturn500_whenMappingJobExecutionCollectionFails() {
    WIRE_MOCK.stubFor(
      WireMock.get(new UrlPathPattern(new RegexPattern("/change-manager/jobExecutions/.{36}/children"), true))
        .willReturn(WireMock.ok().withBody(JsonObject.mapFrom(new JsonObject().put("test", "test")).toString())));

    String id = postRequest(DEFINITION_PATH, UPLOAD_DEF_3)
      .statusCode(HttpStatus.SC_CREATED).log().all().extract().body().jsonPath().get("id");

    deleteRequest(DEFINITION_PATH + "/" + id)
      .statusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR).log().all();
  }

  @DisplayName("should return 422 when upload definition has blocked file extension")
  @Test
  void shouldReturn422_whenUploadDefinitionHasBlockedFileExtension() {
    postRequest(FILE_EXTENSION_DEFAULT, "").log().all()
      .statusCode(HttpStatus.SC_OK).body("totalRecords", is(13));

    postRequest(DEFINITION_PATH, UPLOAD_DEF_6).log().all()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY)
      .body("errors[0].message", is("validation.uploadDefinition.fileExtension.blocked"))
      .body("errors[0].code", is(UPLOAD_DEF_6.getFileDefinitions().getFirst().getName()))
      .body("total_records", is(1));

    postRequest(DEFINITION_PATH, UPLOAD_DEF_7).log().all()
      .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY)
      .body("total_records", is(2));
  }

  private String getUploadDefId(UploadDefinition uploadDefinition) throws IOException {
    String uploadDefId = uploadDefinition.getId();
    String fileId = uploadDefinition.getFileDefinitions().getFirst().getId();

    try (var socket = new Socket("localhost", port);
         var writer = new PrintWriter(socket.getOutputStream())) {
      int falseDataSize = 10;
      writer.print("POST " + DEFINITION_PATH + "/" + uploadDefId + FILE_PATH + "/" + fileId + " HTTP/1.0\r\n"
                   + "Content-Type: application/octet-stream\r\n"
                   + "Accept: application/json,text/plain\r\n"
                   + "x-okapi-tenant: " + TENANT_ID + "\r\n"
                   + "Content-Length: " + falseDataSize + "\r\n"
                   + "\r\n"
                   + "123\r\n");
    }
    return uploadDefId;
  }

  private String getUnsecuredJwtWithUserId(String userId) {
    String header = new JsonObject().put("alg", "none").encode();
    String payload = new JsonObject().put("user_id", userId).put("tenant", TENANT_ID).encode();
    String encodedHeader = Base64.getEncoder().encodeToString(header.getBytes(StandardCharsets.UTF_8));
    String encodedPayload = Base64.getEncoder().encodeToString(payload.getBytes(StandardCharsets.UTF_8));
    return String.format("%s.%s.", encodedHeader, encodedPayload);
  }
}
