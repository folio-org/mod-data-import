package org.folio.rest;

import io.restassured.RestAssured;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class OkapiHealthAPITest extends AbstractRestTest {

  private static final String HEALTH_URL = "/admin/health";

  @Test
  public void shouldBeHealthy() {
    RestAssured
      .given()
      .spec(spec)
      .when()
      .get(HEALTH_URL)
      .then()
      .statusCode(200);
  }
}
