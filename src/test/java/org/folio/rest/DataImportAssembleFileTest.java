package org.folio.rest;


import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;

import javax.net.ssl.HttpsURLConnection;

import org.apache.http.HttpStatus;

import org.folio.rest.jaxrs.model.AssembleFileDto;
import org.folio.rest.jaxrs.model.FileUploadInfo;
import org.folio.s3.client.S3ClientFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.testcontainers.containers.localstack.LocalStackContainer;

import io.restassured.RestAssured;
import io.restassured.mapper.ObjectMapperType;
import io.restassured.path.json.JsonPath;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class DataImportAssembleFileTest extends AbstractRestTest {

  private static final String ASSEMBLE_PATH = "/data-import/assembleStorageFile";
  private static final String UPLOAD_URL_PATH = "/data-import/uploadUrl";
  private static final String UPLOAD_URL_CONTINUE_PATH = "/data-import/uploadUrl/subsequent";
  @Test
  public void shouldAssembleFile(TestContext context) {
    //start upload
  
    JsonPath info1 =  RestAssured.given()
        .spec(spec)
        .when()
        .queryParam("fileName", "test-name1")
        .get(UPLOAD_URL_PATH ).jsonPath();
    String uploadId1 = info1.get("uploadId");
    String key1 = info1.get("key");
   
    
    String url1 = info1.get("url");
    ArrayList<String> tags = putFakeFile(context, url1, 5 * 1024 * 1024);

    //upload 2nd piece
    JsonPath info2 = RestAssured
    .given()
    .spec(spec)
    .when()
    .queryParam("key", key1)
    .queryParam("uploadId",uploadId1)
    .queryParam("partNumber", "2")
    .get(UPLOAD_URL_CONTINUE_PATH)
    .then()
    .statusCode(HttpStatus.SC_OK).log().all()
    .extract().body().jsonPath();

    String url2 = info2.get("url");
 
    tags.addAll( putFakeFile(context, url2,5 * 1024 * 1024));

    AssembleFileDto dto =  new AssembleFileDto();
    dto.setUploadId(uploadId1);
    dto.setKey(key1);
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
  @Test
  public void shouldFailAssembleFileFailedPartUpload(TestContext context) { 
    
    JsonPath info1 =  RestAssured.given()
        .spec(spec)
        .when()
        .queryParam("fileName", "test-name1")
        .get(UPLOAD_URL_PATH ).jsonPath();
    String uploadId1 = info1.get("uploadId");
    String key1 = info1.get("key");
   
    
    String url1 = info1.get("url");
    ArrayList<String> tags = putFakeFile(context, url1, 1 * 1024 * 1024);

    //upload 2nd piece
    JsonPath info2 = RestAssured
    .given()
    .spec(spec)
    .when()
    .queryParam("key", key1)
    .queryParam("uploadId",uploadId1)
    .queryParam("partNumber", "2")
    .get(UPLOAD_URL_CONTINUE_PATH)
    .then()
    .statusCode(HttpStatus.SC_OK).log().all()
    .extract().body().jsonPath();

    String url2 = info2.get("url");
 
    tags.addAll( putFakeFile(context, url2,5 * 1024 * 1024));
    

    AssembleFileDto dto =  new AssembleFileDto();
    dto.setUploadId(uploadId1);
    dto.setKey(key1);
    dto.setTags(tags);
    RestAssured.given()
      .spec(spec)
      .body(dto, ObjectMapperType.GSON)
      .when()
      .post(ASSEMBLE_PATH )
      .then()
      .log().all()
      .statusCode(HttpStatus.SC_BAD_REQUEST);
  }
  private ArrayList<String> putFakeFile(TestContext context, String url1, int size) {
    ArrayList<String> tags = new ArrayList<String>();
    try {
      URL urlobj = new URL(url1);
      HttpURLConnection con = (HttpURLConnection) urlobj.openConnection();
      con.setRequestMethod("PUT");
      con.setDoOutput(true);
      OutputStream output = con.getOutputStream();
      output.write(new byte[size]);
      tags.add(con.getHeaderField("eTag"));
    } catch (Exception e) {
      context.fail(e.getMessage());
    }
    return tags;
  }
}
