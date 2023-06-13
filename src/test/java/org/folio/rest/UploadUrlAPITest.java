package org.folio.rest;

import static org.hamcrest.Matchers.matchesRegex;

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

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  public void testSuccessfulRequest() {
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
        matchesRegex(
          "^http://127\\.0\\.0\\.1:9000/test-bucket/diku/\\d+-test-name\\?X.+$"
        )
      )
      .body("key", matchesRegex("^diku/\\d+-test-name$"));
  }
}
