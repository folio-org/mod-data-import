package org.folio.service.processing.ranking;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Date;
import java.util.Map;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.junit.Test;

public class QueueItemAgeRankerTest extends AbstractQueueItemRankerTest {

  QueueItemAgeRanker ranker;

  public QueueItemAgeRankerTest() {
    this.ranker = new QueueItemAgeRanker(10, 100, 64, -1);
  }

  private DataImportQueueItem ofAge(int age) {
    return new DataImportQueueItem()
      .withTimestamp(Date.from(Instant.now().minus(age, ChronoUnit.MINUTES)));
  }

  @Test
  public void testScoring() {
    assertThat(ranker.score(ofAge(0), Map.of()), is(closeTo(10, EPSILON)));
    assertThat(ranker.score(ofAge(15), Map.of()), is(closeTo(70, EPSILON)));
    assertThat(ranker.score(ofAge(63), Map.of()), is(closeTo(100, EPSILON)));
    assertThat(ranker.score(ofAge(64), Map.of()), is(closeTo(-1, EPSILON)));
    assertThat(ranker.score(ofAge(600), Map.of()), is(closeTo(-1, EPSILON)));
  }
}
