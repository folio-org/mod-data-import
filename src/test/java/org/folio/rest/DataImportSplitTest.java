package org.folio.rest;

import static org.hamcrest.Matchers.is;

import org.apache.http.HttpStatus;
import org.junit.Test;

import io.restassured.RestAssured;

public class DataImportSplitTest extends AbstractRestTest {

  private static final String SPLIT_STATUS_PATH = "/splitStatus";
  
  @Test
  public void shouldRestoreToDefault() {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SPLIT_STATUS_PATH)
      .then()
      .log().all()
      .statusCode(HttpStatus.SC_OK)
      .body("splitStatus", is(true));
  }

}
