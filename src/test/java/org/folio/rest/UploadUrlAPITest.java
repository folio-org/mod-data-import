package org.folio.rest;

import static org.folio.support.TestUtil.UPLOAD_URL_CONTINUE_PATH;
import static org.folio.support.TestUtil.UPLOAD_URL_PATH;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.matchesRegex;
import static org.hamcrest.Matchers.notNullValue;

import java.util.Map;
import org.apache.http.HttpStatus;
import org.folio.support.AbstractRestTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class UploadUrlAPITest extends AbstractRestTest {

  @DisplayName("should return upload URL with part 1 and a key when fileName is provided")
  @Test
  void shouldReturnUploadUrl_whenFirstRequestWithFileName() {
    getRequest(UPLOAD_URL_PATH, Map.of("fileName", "test-name"))
      .statusCode(HttpStatus.SC_OK)
      .body("url", allOf(
        matchesRegex(".*/test-bucket/mod-data-import/data-import/diku/\\d+-test-name.*"),
        containsString("partNumber=1")))
      .body("key", matchesRegex("^data-import/diku/[0-9]+-test-name$"))
      .body("uploadId", notNullValue());
  }

  @DisplayName("should return pre-signed URL for a given part when key, uploadId and partNumber are provided")
  @Test
  void shouldReturnSubsequentUploadUrl_whenKeyAndUploadIdProvided() {
    getRequest(UPLOAD_URL_CONTINUE_PATH, Map.of(
      "key", "data-import/diku/1234-test-name",
      "uploadId", "upload-id-here",
      "partNumber", "5"))
      .statusCode(HttpStatus.SC_OK)
      .body("url", allOf(
        containsString("/test-bucket/mod-data-import/data-import/diku/1234-test-name"),
        containsString("partNumber=5"),
        containsString("uploadId=upload-id-here")))
      .body("key", is(equalTo("data-import/diku/1234-test-name")))
      .body("uploadId", is(equalTo("upload-id-here")));
  }

  @DisplayName("should return 500 when key is outside the permitted folder")
  @Test
  void shouldReturn500_whenKeyOutsidePermittedFolder() {
    getRequest(UPLOAD_URL_CONTINUE_PATH, Map.of(
      "key", "invalid-key-out-of-permitted-folder",
      "uploadId", "upload-id-here",
      "partNumber", "5"))
      .statusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
  }
}
