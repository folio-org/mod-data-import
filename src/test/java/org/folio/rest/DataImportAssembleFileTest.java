package org.folio.rest;


import java.util.ArrayList;

import org.apache.http.HttpStatus;

import org.folio.rest.jaxrs.model.AssembleFileDto;
import org.folio.rest.jaxrs.model.FileUploadInfo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.testcontainers.containers.localstack.LocalStackContainer;

import io.restassured.RestAssured;
import io.restassured.mapper.ObjectMapperType;
import io.restassured.path.json.JsonPath;
import io.restassured.path.json.mapper.factory.JsonbObjectMapperFactory;
import io.restassured.response.Response;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class DataImportAssembleFileTest extends AbstractRestTest {

  private static final String ASSEMBLE_PATH = "/data-import/assembleStorageFile";
  private static final String UPLOAD_URL_PATH = "/data-import/uploadUrl";
  private static final String UPLOAD_URL_CONTINUE_PATH =
      "/data-import/uploadUrl/subsequent";
  @Test
  public void shouldAssembleFileCorrectly(TestContext context) {
    //start upload
    Async async = context.async();
    JsonPath info1 =  RestAssured.given()
        .spec(spec)
        .when()
        .queryParam("fileName", "test-name1")
        .post(UPLOAD_URL_PATH )
        .then()
        .statusCode(HttpStatus.SC_OK)
        .log().all()
        .extract().body().jsonPath();
    String uploadId1 = info1.get("uploadId");
    String key1 = info1.get("key");
    
    async.complete();
    async = context.async();
    //upload 1st piece
   
    //upload second piece
  
  
    
  
    ArrayList<String> tags = new ArrayList<String>();

    AssembleFileDto dto =  new AssembleFileDto();
    dto.setUploadId("aosidfpvxcohujasleih");
    dto.setKey("this/is/a/key");
    dto.setTags(tags);
    RestAssured.given()
      .spec(spec)
      .body(dto, ObjectMapperType.GSON)
      .when()
      .post(ASSEMBLE_PATH )
      .then()
      .log().all()
      .statusCode(HttpStatus.SC_NO_CONTENT);
  }
}
