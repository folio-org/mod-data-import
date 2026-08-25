package org.folio.service.processing.split;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class FileSplitUtilitiesChunkKeyTest {

  static Stream<Arguments> getCases() {
    return Stream.of(
      Arguments.of("test", 1, "test_1"),
      Arguments.of("test.mrc", 1, "test_1.mrc"),
      Arguments.of("test.mrc", 234, "test_234.mrc"),
      Arguments.of("test.foo.mrc", 1, "test.foo_1.mrc"),
      Arguments.of("test.foo_12.mrc", 2, "test.foo_12_2.mrc"),
      Arguments.of("a/really.long/and_..complex.path", 15, "a/really.long/and_..complex_15.path"),
      Arguments.of("windows\\style.path", 128, "windows\\style_128.path")
    );
  }

  @ParameterizedTest
  @MethodSource("getCases")
  @DisplayName("should build chunk key from input key and part number")
  void shouldBuildChunkKey(String key, int partNumber, String expected) {
    assertThat(FileSplitUtilities.buildChunkKey(key, partNumber)).isEqualTo(expected);
  }
}
