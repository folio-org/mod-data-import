package org.folio.service.processing.ranking;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;

import java.util.Map;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class QueueItemTenantUsageRankerTest
  extends AbstractQueueItemRankerTest {

  QueueItemTenantUsageRanker ranker;

  QueueItemTenantUsageRankerTest() {
    this.ranker = new QueueItemTenantUsageRanker(100, -100);
  }

  @DisplayName("should return correct score based on tenant usage")
  @Test
  void shouldReturnCorrectScore_whenTenantUsageVaries() {
    DataImportQueueItem job = new DataImportQueueItem().withTenant("A");

    assertThat(ranker.score(job, Map.of())).isCloseTo(100, within(EPSILON));
    assertThat(ranker.score(job, Map.of("A", 0L))).isCloseTo(100, within(EPSILON));
    assertThat(ranker.score(job, Map.of("A", 1L))).isCloseTo(-100, within(EPSILON));
    assertThat(ranker.score(job, Map.of("B", 2L))).isCloseTo(100, within(EPSILON));
    assertThat(
      ranker.score(job, Map.of("A", 0L, "B", 2L))
    ).isCloseTo(100, within(EPSILON));
    assertThat(
      ranker.score(job, Map.of("A", 1L, "B", 3L))
    ).isCloseTo(50, within(EPSILON));
    assertThat(
      ranker.score(job, Map.of("A", 2L, "B", 2L))
    ).isCloseTo(0, within(EPSILON));
    assertThat(
      ranker.score(job, Map.of("A", 3L, "B", 1L))
    ).isCloseTo(-50, within(EPSILON));
  }
}
