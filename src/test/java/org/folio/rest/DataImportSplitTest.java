package org.folio.rest;

import static org.hamcrest.Matchers.is;

import org.apache.http.HttpStatus;
import org.junit.Test;
import org.junit.runner.RunWith;

import io.restassured.RestAssured;
import io.vertx.ext.unit.junit.VertxUnitRunner;

@RunWith(VertxUnitRunner.class)
public class DataImportSplitTest extends AbstractRestTest {

  private static final String SPLIT_STATUS_PATH = "/data-import/splitStatus";
  
  @Test
  public void shouldGetDefaultSplit() {
    RestAssured.given()
      .spec(spec)
      .when()
      .get(SPLIT_STATUS_PATH)
      .then()
      .log().all()
      .statusCode(HttpStatus.SC_OK)
      .body("splitStatus", is(false));
  }

}
