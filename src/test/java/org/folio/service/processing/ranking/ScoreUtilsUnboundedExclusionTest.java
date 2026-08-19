package org.folio.service.processing.ranking;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ScoreUtilsUnboundedExclusionTest {

  @DisplayName("should return zero when excluded metrics are provided")
  @Test
  void shouldReturnZero_whenExcludedMetricsAreProvided() {
    assertThat(
      ScoreUtils.calculateUnboundedLogarithmicScore(0, 0, 0, 0)
    ).isEqualTo(0d);
    assertThat(
      ScoreUtils.calculateUnboundedLogarithmicScore(10, 0, 0, 0)
    ).isEqualTo(0d);
    assertThat(
      ScoreUtils.calculateUnboundedLogarithmicScore(10, 0, 10, 0)
    ).isEqualTo(0d);
    assertThat(
      ScoreUtils.calculateUnboundedLogarithmicScore(10, 0, 0, 10)
    ).isEqualTo(0d);
  }
}
