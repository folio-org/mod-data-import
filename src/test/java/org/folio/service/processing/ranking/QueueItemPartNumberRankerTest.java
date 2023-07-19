package org.folio.service.processing.ranking;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;

import java.util.Map;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.junit.Test;

public class QueueItemPartNumberRankerTest extends AbstractQueueItemRankerTest {

  QueueItemPartNumberRanker ranker;

  public QueueItemPartNumberRankerTest() {
    this.ranker = new QueueItemPartNumberRanker();

    this.setField(ranker, "scoreFirst", 1);
    this.setField(ranker, "scoreLast", 0);
    this.setField(ranker, "scoreLastReference", 31);
  }

  private DataImportQueueItem ofPart(int partNumber) {
    return new DataImportQueueItem().withPartNumber(partNumber);
  }

  @Test
  public void testScoring() {
    assertThat(ranker.score(ofPart(1), Map.of()), is(closeTo(1, EPSILON)));
    assertThat(ranker.score(ofPart(2), Map.of()), is(closeTo(0.8, EPSILON)));
    assertThat(ranker.score(ofPart(4), Map.of()), is(closeTo(0.6, EPSILON)));
    assertThat(ranker.score(ofPart(8), Map.of()), is(closeTo(0.4, EPSILON)));
    assertThat(ranker.score(ofPart(16), Map.of()), is(closeTo(0.2, EPSILON)));
    assertThat(ranker.score(ofPart(32), Map.of()), is(closeTo(0, EPSILON)));
    assertThat(ranker.score(ofPart(64), Map.of()), is(closeTo(-0.2, EPSILON)));
  }
}
