package org.folio.rest;

import static org.hamcrest.Matchers.is;

import io.restassured.RestAssured;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.http.HttpStatus;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class SplitStatusEnabledTest extends AbstractRestTest {

  private static final String SPLIT_STATUS_PATH = "/data-import/splitStatus";

  @BeforeClass
  public static void configureEnv() {
    System.setProperty("SPLIT_FILES_ENABLED", "true");
  }

  @Test
  public void testEnabledSplit() {
    RestAssured
      .given()
      .spec(spec)
      .when()
      .get(SPLIT_STATUS_PATH)
      .then()
      .log()
      .all()
      .statusCode(HttpStatus.SC_OK)
      .body("splitStatus", is(true));
  }

  @AfterClass
  public static void resetEnv() {
    System.clearProperty("SPLIT_FILES_ENABLED");
  }
}
