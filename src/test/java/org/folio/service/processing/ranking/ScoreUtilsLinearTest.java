package org.folio.service.processing.ranking;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ScoreUtilsLinearTest {

  private static final int LOWER_SCORE = 0;
  private static final int UPPER_SCORE = 100;

  private static final double EPSILON = 0.0000001;

  static Stream<Arguments> getExpectedValues() {
    return Stream.of(
      Arguments.of(0.0, 0.0),
      Arguments.of(0.1, 10.0),
      Arguments.of(0.2, 20.0),
      Arguments.of(0.3, 30.0),
      Arguments.of(0.4, 40.0),
      Arguments.of(0.5, 50.0),
      Arguments.of(0.6, 60.0),
      Arguments.of(0.7, 70.0),
      Arguments.of(0.8, 80.0),
      Arguments.of(0.9, 90.0),
      Arguments.of(1.0, 100.0)
    );
  }

  @ParameterizedTest(name = "[{index}] value={0} → expected={1}")
  @MethodSource("getExpectedValues")
  @DisplayName("should return linear score for given value")
  void shouldReturnLinearScore_forGivenValue(double value, double expected) {
    assertThat(ScoreUtils.calculateLinearScore(value, LOWER_SCORE, UPPER_SCORE))
      .isCloseTo(expected, within(EPSILON));
  }
}
