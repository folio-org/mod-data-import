package org.folio.rest;

import com.github.tomakehurst.wiremock.client.WireMock;
import io.restassured.RestAssured;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.folio.rest.jaxrs.model.AssembleFileDto;
import org.folio.rest.jaxrs.model.FileDefinition;
import org.folio.rest.jaxrs.model.FileUploadInfo;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.UploadDefinition;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.matchingJsonPath;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.fail;

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
    UploadDefinition definition = createUploadDefinition();

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

    AssembleFileDto dto = new AssembleFileDto()
      .withKey(firstPartUploadInfo.getKey())
      .withUploadId(firstPartUploadInfo.getUploadId())
      .withTags(tags);

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

  @Test
  public void shouldFailAssembleFileFailedPartUpload(TestContext context) {
    UploadDefinition definition = createUploadDefinition();

    FileUploadInfo firstPartUploadInfo = getFirstPart("test-name1");

    // no parts actually uploaded
    AssembleFileDto dto = new AssembleFileDto()
      .withKey(firstPartUploadInfo.getKey())
      .withUploadId(firstPartUploadInfo.getUploadId())
      .withTags(Arrays.asList("invalid"));

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
      .statusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);

    UploadDefinition result = getUploadDefinition(definition.getId());

    assertThat(
      result.getFileDefinitions().get(0).getSourcePath(),
      is(nullValue())
    );
    assertThat(
      result.getFileDefinitions().get(0).getStatus(),
      is(FileDefinition.Status.UPLOADING)
    );
  }

  private UploadDefinition createUploadDefinition() {
    WireMock.stubFor(
      put(
        urlPathMatching(
          "/change-manager/jobExecutions/" + JOB_EXEC_ID + "/status"
        )
      )
        .withRequestBody(matchingJsonPath("$.status", equalTo("FILE_UPLOADED")))
        .willReturn(okJson(JsonObject.mapFrom(new JobExecution()).toString()))
    );

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
    return JsonObject
      .mapFrom(
        RestAssured
          .given()
          .spec(spec)
          .when()
          .get(DEFINITION_PATH + "/" + id)
          .then()
          .statusCode(HttpStatus.SC_OK)
          .extract()
          .as(UploadDefinition.class)
      )
      .mapTo(UploadDefinition.class);
  }

  private FileUploadInfo getFirstPart(String filename) {
    return RestAssured
      .given()
      .spec(spec)
      .when()
      .queryParam("fileName", filename)
      .get(UPLOAD_URL_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .extract()
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
      .then()
      .statusCode(HttpStatus.SC_OK)
      .extract()
      .body()
      .as(FileUploadInfo.class);
  }

  private String upload(String url, int size) {
    // unsure how to make this work with restassured...
    try {
      HttpURLConnection con = (HttpURLConnection) new URL(url).openConnection();
      con.setRequestMethod("PUT");
      con.setDoOutput(true);
      OutputStream output = con.getOutputStream();
      output.write(new byte[size]);
      return (con.getHeaderField("eTag"));
    } catch (Exception e) {
      fail(e.getMessage());
      throw new IllegalStateException();
    }
  }
}
