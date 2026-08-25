package org.folio.service.processing.ranking;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class ScoreUtilsBoundedTest {

  private static final int LOWER_SCORE = 0;
  private static final int UPPER_SCORE = 5;
  private static final int THRESHOLD = 32;
  private static final int THRESHOLD_SCORE = -1;

  private static final double EPSILON = 0.0000001;

  static Stream<Arguments> getExpectedValues() {
    return Stream.of(
      Arguments.of(0, 0, 0, 0),
      Arguments.of(1, 1, 0, 1),
      Arguments.of(2, 3, 1, 2),
      Arguments.of(4, 7, 2, 3),
      Arguments.of(8, 15, 3, 4),
      Arguments.of(16, 31, 4, 5),
      Arguments.of(32, 100, -1, -1)
    );
  }

  @ParameterizedTest(name = "[{index}] i in [{0},{1}] → score in [{2},{3}]")
  @MethodSource("getExpectedValues")
  @DisplayName("should score within bounds for all i in range")
  void shouldScoreWithinBounds_forAllInRange(int lowerRange, int upperRange, int lowerScore, int upperScore) {
    assertThat(IntStream.rangeClosed(lowerRange, upperRange).asDoubleStream()
      .map(i -> ScoreUtils.calculateBoundedLogarithmicScore((int) i,
        LOWER_SCORE, UPPER_SCORE, THRESHOLD, THRESHOLD_SCORE)))
      .isNotEmpty()
      .allSatisfy(score -> assertThat(score).isBetween(lowerScore - EPSILON, upperScore + EPSILON));
  }
}
