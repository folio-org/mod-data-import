package org.folio.rest;

import static org.folio.support.TestUtil.FILE_EXTENSIONS_PATH;
import static org.hamcrest.Matchers.is;

import org.apache.http.HttpStatus;
import org.folio.support.AbstractRestTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class DefaultFileExtensionRestTest extends AbstractRestTest {

  static final String FILE_EXTENSION_DEFAULT = FILE_EXTENSIONS_PATH + "/restore/default";

  @DisplayName("should restore 13 default file extensions when restore endpoint is called")
  @Test
  void shouldRestoreToDefault() {
    postRequest(FILE_EXTENSION_DEFAULT, "")
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(13));

    getRequest(FILE_EXTENSIONS_PATH)
      .statusCode(HttpStatus.SC_OK)
      .body("totalRecords", is(13));
  }
}
