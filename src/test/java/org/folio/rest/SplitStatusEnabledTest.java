package org.folio.rest;

import static org.hamcrest.Matchers.is;

import org.apache.http.HttpStatus;
import org.folio.support.AbstractRestTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class SplitStatusEnabledTest extends AbstractRestTest {

  // static initializer runs before BaseRestTest.deployRestVerticle() @BeforeAll,
  // so the verticle picks up SPLIT_FILES_ENABLED=true at startup
  static {
    System.setProperty("SPLIT_FILES_ENABLED", "true");
  }

  private static final String SPLIT_STATUS_PATH = "/data-import/splitStatus";

  @AfterAll
  static void resetEnv() {
    System.clearProperty("SPLIT_FILES_ENABLED");
  }

  @DisplayName("should return splitStatus=true when SPLIT_FILES_ENABLED is set to true")
  @Test
  void shouldReturnTrue_whenSplitFilesEnabledIsTrue() {
    getRequest(SPLIT_STATUS_PATH)
      .statusCode(HttpStatus.SC_OK)
      .body("splitStatus", is(true));
  }
}
