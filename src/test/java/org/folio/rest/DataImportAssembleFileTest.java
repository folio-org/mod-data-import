package org.folio.rest;

import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import com.github.tomakehurst.wiremock.client.WireMock;
import io.restassured.RestAssured;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.http.HttpStatus;
import org.folio.rest.jaxrs.model.AssembleFileDto;
import org.folio.rest.jaxrs.model.FileDefinition;
import org.folio.rest.jaxrs.model.FileUploadInfo;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.UploadDefinition;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class DataImportAssembleFileTest extends AbstractRestTest {

  private static final String DEFINITION_PATH =
    "/data-import/uploadDefinitions";

  private static final String ASSEMBLE_PATH =
    "/data-import/uploadDefinitions/{uploadDefinitionId}/files/{fileDefinitionId}/assembleStorageFile";

  private static final String UPLOAD_URL_PATH = "/data-import/uploadUrl";
  private static final String UPLOAD_URL_CONTINUE_PATH =
    "/data-import/uploadUrl/subsequent";

  private static final String JOB_EXEC_ID =
    "90e5a90e-4133-563c-ab33-969b39080c1c";

  @Test
  public void testAssemble(TestContext context) {
    FileUploadInfo firstPartUploadInfo = getFirstPart("test-name1");
    FileUploadInfo secondPartUploadInfo = getLaterPart(
      firstPartUploadInfo.getKey(),
      firstPartUploadInfo.getUploadId(),
      2
    );
    FileUploadInfo thirdPartUploadInfo = getLaterPart(
      firstPartUploadInfo.getKey(),
      firstPartUploadInfo.getUploadId(),
      3
    );

    List<String> tags = new ArrayList<>();
    tags.add(upload(firstPartUploadInfo.getUrl(), 5 * 1024 * 1024));
    tags.add(upload(secondPartUploadInfo.getUrl(), 5 * 1024 * 1024));
    tags.add(upload(thirdPartUploadInfo.getUrl(), 3 * 1024 * 1024));

    System.out.println(tags.stream().collect(Collectors.joining(", ")));

    UploadDefinition definition = createUploadDefinition();

    AssembleFileDto dto = new AssembleFileDto()
      .withKey(firstPartUploadInfo.getKey())
      .withUploadId(firstPartUploadInfo.getUploadId())
      .withTags(tags);

    WireMock.stubFor(
      put(
        urlPathMatching(
          "/change-manager/jobExecutions/" + JOB_EXEC_ID + "/status"
        )
      )
        .withRequestBody(matchingJsonPath("$.status", equalTo("FILE_UPLOADED")))
        .willReturn(okJson(JsonObject.mapFrom(new JobExecution()).toString()))
    );

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

    UploadDefinition result = getUploadDefinition(definition.getId());

    assertThat(
      result.getFileDefinitions().get(0).getSourcePath(),
      is(firstPartUploadInfo.getKey())
    );
    assertThat(
      result.getFileDefinitions().get(0).getStatus(),
      is(FileDefinition.Status.UPLOADED)
    );
  }

  // @Test
  // public void shouldFailAssembleFileFailedPartUpload(TestContext context) {
  //   JsonPath info1 = RestAssured
  //     .given()
  //     .spec(spec)
  //     .when()
  //     .queryParam("fileName", "test-name1")
  //     .get(UPLOAD_URL_PATH)
  //     .jsonPath();
  //   String uploadId1 = info1.get("uploadId");
  //   String key1 = info1.get("key");

  //   String url1 = info1.get("url");
  //   ArrayList<String> tags = putFakeFile(context, url1, 1 * 1024 * 1024);

  //   //upload 2nd piece
  //   JsonPath info2 = RestAssured
  //     .given()
  //     .spec(spec)
  //     .when()
  //     .queryParam("key", key1)
  //     .queryParam("uploadId", uploadId1)
  //     .queryParam("partNumber", "2")
  //     .get(UPLOAD_URL_CONTINUE_PATH)
  //     .then()
  //     .statusCode(HttpStatus.SC_OK)
  //     .log()
  //     .all()
  //     .extract()
  //     .body()
  //     .jsonPath();

  //   String url2 = info2.get("url");

  //   tags.addAll(putFakeFile(context, url2, 5 * 1024 * 1024));

  //   AssembleFileDto dto = new AssembleFileDto();
  //   dto.setUploadId(uploadId1);
  //   dto.setKey(key1);
  //   dto.setTags(tags);
  //   RestAssured
  //     .given()
  //     .spec(spec)
  //     .body(dto, ObjectMapperType.GSON)
  //     .when()
  //     .post(ASSEMBLE_PATH)
  //     .then()
  //     .log()
  //     .all()
  //     .statusCode(HttpStatus.SC_BAD_REQUEST);
  // }

  private UploadDefinition createUploadDefinition() {
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

  private UploadDefinition getUploadDefinition(String id) {
    return RestAssured
      .given()
      .spec(spec)
      .when()
      .get(DEFINITION_PATH + "?query=id==" + id)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(1))
      .extract()
      .path("uploadDefinitions[0]");
  }

  private FileUploadInfo getFirstPart(String filename) {
    return RestAssured
      .given()
      .spec(spec)
      .when()
      .queryParam("fileName", filename)
      .get(UPLOAD_URL_PATH)
      .thenReturn()
      .body()
      .as(FileUploadInfo.class);
  }

  private FileUploadInfo getLaterPart(
    String key,
    String uploadId,
    int partNumber
  ) {
    return RestAssured
      .given()
      .spec(spec)
      .when()
      .queryParam("key", key)
      .queryParam("uploadId", uploadId)
      .queryParam("partNumber", Integer.toString(partNumber))
      .get(UPLOAD_URL_CONTINUE_PATH)
      .thenReturn()
      .body()
      .as(FileUploadInfo.class);
  }

  private String upload(String url, int size) {
    return RestAssured
      .given()
      .spec(spec)
      .body(new byte[size])
      .when()
      .post(url)
      .then()
      .log()
      .all()
      .statusCode(200)
      .extract()
      .headers()
      .toString();
  }
}
