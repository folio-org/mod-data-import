package org.folio.service.processing.ranking;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import org.junit.Test;

public class ScoreUtilsUnboundedExclusionTest {

  @Test
  public void testExcludedMetrics() {
    assertThat(
      ScoreUtils.calculateUnboundedLogarithmicScore(0, 0, 0, 0),
      is(0d)
    );
    assertThat(
      ScoreUtils.calculateUnboundedLogarithmicScore(10, 0, 0, 0),
      is(0d)
    );
    assertThat(
      ScoreUtils.calculateUnboundedLogarithmicScore(10, 0, 10, 0),
      is(0d)
    );
    assertThat(
      ScoreUtils.calculateUnboundedLogarithmicScore(10, 0, 0, 10),
      is(0d)
    );
  }
}
