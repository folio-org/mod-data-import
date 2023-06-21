package org.folio.rest;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.matchesRegex;
import static org.hamcrest.Matchers.notNullValue;

import io.restassured.RestAssured;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockitoAnnotations;

@RunWith(VertxUnitRunner.class)
public class UploadUrlAPITest extends AbstractRestTest {

  private static final String UPLOAD_URL_PATH = "/data-import/uploadUrl";
  private static final String UPLOAD_URL_CONTINUE_PATH =
    "/data-import/uploadUrl/continue";

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  public void testSuccessfulFirstRequest() {
    RestAssured
      .given()
      .spec(spec)
      .when()
      .queryParam("fileName", "test-name")
      .get(UPLOAD_URL_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body(
        "url",
        allOf(
          matchesRegex(".*/test-bucket/diku/\\d+-test-name.*"),
          containsString("partNumber=1")
        )
      )
      .body("key", matchesRegex("^diku/[0-9]+-test-name$"))
      .body("uploadId", notNullValue());
  }

  @Test
  public void testSuccessfulLaterRequest() {
    RestAssured
      .given()
      .spec(spec)
      .when()
      .queryParam("key", "diku/1234-test-name")
      .queryParam("uploadId", "upload-id-here")
      .queryParam("partNumber", "5")
      .get(UPLOAD_URL_CONTINUE_PATH)
      .then()
      .statusCode(HttpStatus.SC_OK)
      .body(
        "url",
        allOf(
          containsString("/test-bucket/diku/1234-test-name"),
          containsString("partNumber=5"),
          containsString("uploadId=upload-id-here")
        )
      )
      .body("key", is(equalTo("diku/1234-test-name")))
      .body("uploadId", is(equalTo("upload-id-here")));
  }
}
