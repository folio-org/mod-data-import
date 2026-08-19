package org.folio.service.processing.ranking;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import java.util.Map;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class QueueItemSizeRankerTest extends AbstractQueueItemRankerTest {

  QueueItemSizeRanker ranker;

  public QueueItemSizeRankerTest() {
    this.ranker = new QueueItemSizeRanker(100, 10, 63);
  }

  private DataImportQueueItem ofSize(int size) {
    return new DataImportQueueItem().withOriginalSize(size);
  }

  @DisplayName("should return correct score based on item size")
  @Test
  void shouldReturnCorrectScore_whenItemSizeVaries() {
    assertThat(ranker.score(ofSize(0), Map.of())).isCloseTo(100, within(EPSILON));
    assertThat(ranker.score(ofSize(15), Map.of())).isCloseTo(40, within(EPSILON));
    assertThat(ranker.score(ofSize(63), Map.of())).isCloseTo(10, within(EPSILON));
    assertThat(ranker.score(ofSize(127), Map.of())).isCloseTo(-5, within(EPSILON));
  }
}
