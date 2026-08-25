package org.folio.rest;

import static org.hamcrest.Matchers.is;

import org.apache.http.HttpStatus;
import org.folio.support.AbstractRestTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class SplitStatusDefaultTest extends AbstractRestTest {

  private static final String SPLIT_STATUS_PATH = "/data-import/splitStatus";

  @DisplayName("should return splitStatus=false when SPLIT_FILES_ENABLED is not set")
  @Test
  void shouldReturnFalse_whenSplitFilesEnabledNotSet() {
    getRequest(SPLIT_STATUS_PATH)
      .statusCode(HttpStatus.SC_OK)
      .body("splitStatus", is(false));
  }
}
