package org.folio.service.processing.ranking;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;

import java.util.Arrays;
import java.util.Collection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class ScoreUtilsLinearTest {

  private static final int LOWER_SCORE = 0;
  private static final int UPPER_SCORE = 100;

  private static final double EPSILON = 0.0000001;

  /**
   * returns values that should have {@code score(value)=result}
   * @return tuples [value, result]
   */
  @Parameters
  public static Collection<Object[]> getExpectedValues() {
    return Arrays.asList(
      new Object[] { 0, 0 },
      new Object[] { 0.1, 10 },
      new Object[] { 0.2, 20 },
      new Object[] { 0.3, 30 },
      new Object[] { 0.4, 40 },
      new Object[] { 0.5, 50 },
      new Object[] { 0.6, 60 },
      new Object[] { 0.7, 70 },
      new Object[] { 0.8, 80 },
      new Object[] { 0.9, 90 },
      new Object[] { 1, 100 }
    );
  }

  private double value;
  private double expected;

  public ScoreUtilsLinearTest(double value, double expected) {
    this.value = value;
    this.expected = expected;
  }

  @Test
  public void test() {
    assertThat(
      ScoreUtils.calculateLinearScore(value, LOWER_SCORE, UPPER_SCORE),
      is(closeTo(expected, EPSILON))
    );
  }
}
