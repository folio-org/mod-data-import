package org.folio.service.processing.ranking;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;

import java.util.Map;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.junit.Test;

public class QueueItemSizeRankerTest extends AbstractQueueItemRankerTest {

  QueueItemSizeRanker ranker;

  public QueueItemSizeRankerTest() {
    this.ranker = new QueueItemSizeRanker();

    this.setField(ranker, "scoreSmallest", 100);
    this.setField(ranker, "scoreLargest", 10);
    this.setField(ranker, "scoreLargeReference", 63);
  }

  private DataImportQueueItem ofSize(int size) {
    return new DataImportQueueItem().withOriginalSize(size);
  }

  @Test
  public void testScoring() {
    assertThat(ranker.score(ofSize(0), Map.of()), is(closeTo(100, EPSILON)));
    assertThat(ranker.score(ofSize(15), Map.of()), is(closeTo(40, EPSILON)));
    assertThat(ranker.score(ofSize(63), Map.of()), is(closeTo(10, EPSILON)));
    assertThat(ranker.score(ofSize(127), Map.of()), is(closeTo(-5, EPSILON)));
  }
}
