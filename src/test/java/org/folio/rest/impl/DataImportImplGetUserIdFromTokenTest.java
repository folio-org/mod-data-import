package org.folio.rest.impl;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Base64;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class DataImportImplGetUserIdFromTokenTest {

  @ParameterizedTest
  @MethodSource("getExpectedValues")
  @DisplayName("should extract user ID from JWT token")
  void shouldExtractUserId_fromToken(String token, String expected) {
    assertThat(DataImportImpl.getUserIdFromToken(token)).isEqualTo(expected);
  }

  private static Stream<Arguments> getExpectedValues() {
    return Stream.of(
      Arguments.of(null, null),
      Arguments.of("foo", null),
      Arguments.of("jwt.invalid", null),
      Arguments.of("jwt." + base64Encode("invalid"), null),
      Arguments.of("jwt." + base64Encode("{}"), null),
      Arguments.of("jwt." + base64Encode("{\"user_id\":\"foo\"}"), "foo")
    );
  }

  private static String base64Encode(String plaintext) {
    return Base64.getEncoder().encodeToString(plaintext.getBytes());
  }
}
