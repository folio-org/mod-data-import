package org.folio.rest;

import com.jayway.restassured.RestAssured;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.junit.Test;
import org.junit.runner.RunWith;

import static org.hamcrest.Matchers.is;

@RunWith(VertxUnitRunner.class)
public class DefaultFileExtensionAPITest extends AbstractRestTest {

  private static final String FILE_EXTENSION_PATH = "/data-import/fileExtensions";
  private static final String FILE_EXTENSION_DEFAULT = FILE_EXTENSION_PATH + "/restore/default";

  @Test
  public void shouldRestoreToDefault() {
    RestAssured.given()
      .spec(spec)
      .when()
      .post(FILE_EXTENSION_DEFAULT)
      .then()
      .log().all()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(16));

    RestAssured.given()
      .spec(spec)
      .when()
      .get(FILE_EXTENSION_PATH)
      .then()
      .log().all()
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(16));
  }

}
