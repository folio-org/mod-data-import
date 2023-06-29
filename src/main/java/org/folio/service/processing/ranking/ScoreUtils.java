package org.folio.service.processing.ranking;

import javax.validation.constraints.Min;

public class ScoreUtils {

  private ScoreUtils() {
    throw new UnsupportedOperationException(
      "Cannot instantiate utility class."
    );
  }

  /**
   * Calculate the logarithm-based score for a given value [0,inf).
   *
   * For example, if we expect {@code value} to be from 0-31 (reference=31),
   * {@value lowerScore=0}, and {@value upperScore=5}:
   *
   * <pre>
   * Values   | Score
   * [0,1]   | [0, 1]
   * [2,3]   | (1, 2]
   * [4,7]   | (2, 3]
   * [8,15]  | (3, 4]
   * [16,31] | (4, 5]
   * [32,63] | (5, 6]
   * ...
   * </pre>
   *
   * @param value the value to calculate the score for; MUST be greater than or
   *              equal to 0. Typically is less than {@code upperReference},
   *              but it can go as large as +Infinity
   * @param lowerScore the score to assign to a value of 0
   * @param upperScore the score to assign to a value of {@code upperReference}
   * @param upperReference the typical upper bound of the value.  This does not
   *                       need to be an absolute bound, since {@code value}
   *                       can go to Infinity, however, it should be the
   *                       "typical" or "expected" maximum value
   * @return the logarithm-based score for the given value
   */
  public static double calculateUnboundedLogarithmicScore(
    @Min(0) int value,
    int lowerScore,
    int upperScore,
    @Min(0) int upperReference
  ) {
    // we must add 1 to the value to avoid taking the log of 0
    double upperLog = Math.log(upperReference + 1);
    double valueLog = Math.log(value + 1);

    double proportion = valueLog / upperLog;
    double scoreRange = upperScore - lowerScore;

    return lowerScore + (proportion * scoreRange);
  }

  /**
   * Calculate the logarithm-based score for a given value [0,inf), with a
   * fallback to prevent this value from getting "too large".
   *
   * An example usage of this could be for a job's age: we want to increase
   * the score as the job ages, however, if it exceeds a given amount of time,
   * it should be given the utmost priority and bypass the traditional
   * calculation.
   *
   * For example, if we expect {@code value} to be from 0-31 (threshold=31),
   * {@value lowerScore=0}, {@value upperScore=5}, and
   * {@code upperThresholdScore=100}:
   *
   * <pre>
   * Values    | Score
   * [0,1]    | [0, 1]
   * [2,3]    | (1, 2]
   * [4,7]    | (2, 3]
   * [8,15]   | (3, 4]
   * [16,31]  | (4, 5]
   * (31,inf) | 100
   * ...
   * </pre>
   *
   * @param value the value to calculate the score for; MUST be greater than or
   *              equal to 0. Typically is less than {@code upperReference},
   *              but it can go as large as +Infinity
   * @param lowerScore the score to assign to a value of 0
   * @param upperScore the score to assign to a value of {@code upperReference}
   * @param upperThreshold the typical upper bound of the value.  This does not
   *                       need to be an absolute bound, since {@code value}
   *                       can go to Infinity, however, it should be the
   *                       "typical" or "expected" maximum value
   * @param upperThresholdScore the score to give values that exceed
   *                            {@code upperThreshold}
   * @return the bounded logarithm-based score for the given value
   */
  public static double calculateBoundedLogarithmicScore(
    @Min(0) int value,
    int lowerScore,
    int upperScore,
    @Min(0) int upperThreshold,
    int upperThresholdScore
  ) {
    if (value >= upperThreshold) {
      return upperThresholdScore;
    }

    return calculateUnboundedLogarithmicScore(
      value,
      lowerScore,
      upperScore,
      upperThreshold
    );
  }

  /**
   * Calculate the linear score for an {@code value} between 0 and 1
   * (inclusive).
   *
   * An example usage of this could be for a tenant's current usage, as it
   * has a known bound and this proportion would range from 0 to 100% (1).
   *
   * @param value the value to calculate the score for; MUST be between zero
   *              and one (inclusive).
   * @param lowerScore the score to assign to a value of 0
   * @param upperScore the score to assign to a value of 1
   * @return the linear score for the given value
   */
  public static double calculateLinearScore(
    double value,
    int lowerScore,
    int upperScore
  ) {
    return lowerScore + (value * (upperScore - lowerScore));
  }
}
