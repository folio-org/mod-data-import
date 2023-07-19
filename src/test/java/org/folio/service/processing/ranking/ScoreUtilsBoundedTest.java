package org.folio.service.processing.ranking;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

import java.util.Arrays;
import java.util.Collection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class ScoreUtilsBoundedTest {

  private static final int LOWER_SCORE = 0;
  private static final int UPPER_SCORE = 5;
  private static final int THRESHOLD = 32;
  private static final int THRESHOLD_SCORE = -1;

  private static final double EPSILON = 0.0000001;

  /**
   * returns values that, for all between start and end I (inclusive), should
   * have scores between lower bound and upper bound
   * @return tuples [start I, end I, lower bound, upper bound]
   */
  @Parameters
  public static Collection<Object[]> getExpectedValues() {
    return Arrays.asList(
      new Object[] { 0, 0, 0, 0 },
      new Object[] { 1, 1, 0, 1 },
      new Object[] { 2, 3, 1, 2 },
      new Object[] { 4, 7, 2, 3 },
      new Object[] { 8, 15, 3, 4 },
      new Object[] { 16, 31, 4, 5 },
      new Object[] { 32, 100, -1, -1 }
    );
  }

  private int lowerRange;
  private int upperRange;
  private int lowerScore;
  private int upperScore;

  public ScoreUtilsBoundedTest(
    int lowerRange,
    int upperRange,
    int lowerScore,
    int upperScore
  ) {
    this.lowerRange = lowerRange;
    this.upperRange = upperRange;
    this.lowerScore = lowerScore;
    this.upperScore = upperScore;
  }

  @Test
  public void testScoring() {
    for (int i = lowerRange; i <= upperRange; i++) {
      assertThat(
        ScoreUtils.calculateBoundedLogarithmicScore(
          i,
          LOWER_SCORE,
          UPPER_SCORE,
          THRESHOLD,
          THRESHOLD_SCORE
        ),
        is(
          both(greaterThanOrEqualTo(lowerScore - EPSILON))
            .and(lessThanOrEqualTo(upperScore + EPSILON))
        )
      );
    }
  }
}
