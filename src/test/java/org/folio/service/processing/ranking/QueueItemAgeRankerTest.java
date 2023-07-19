package org.folio.service.processing.ranking;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;

import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class QueueItemAgeRankerTest extends AbstractQueueItemRankerTest {

  private static final double EPSILON = 0.0000001;

  QueueItemAgeRanker ageRanker;

  public QueueItemAgeRankerTest() {
    this.ageRanker = new QueueItemAgeRanker();

    this.setField(ageRanker, "scoreAgeNewest", 10);
    this.setField(ageRanker, "scoreAgeOldest", 100);
    this.setField(ageRanker, "scoreAgeExtremeThresholdMinutes", 64);
    this.setField(ageRanker, "scoreAgeExtremeValue", -1);
  }

  private DataImportQueueItem ofAge(int age) {
    return new DataImportQueueItem()
      .withTimestamp(Instant.now().minus(age, ChronoUnit.MINUTES).toString());
  }

  @Test
  public void testScoring() {
    assertThat(ageRanker.score(ofAge(0), Map.of()), is(closeTo(10, EPSILON)));
    assertThat(ageRanker.score(ofAge(15), Map.of()), is(closeTo(70, EPSILON)));
    assertThat(ageRanker.score(ofAge(63), Map.of()), is(closeTo(100, EPSILON)));
    assertThat(ageRanker.score(ofAge(64), Map.of()), is(closeTo(-1, EPSILON)));
    assertThat(ageRanker.score(ofAge(600), Map.of()), is(closeTo(-1, EPSILON)));
  }
}
