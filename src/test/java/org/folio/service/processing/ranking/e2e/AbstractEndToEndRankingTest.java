package org.folio.service.processing.ranking.e2e;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableSet;
import java.util.UUID;
import org.folio.dao.DataImportQueueItemDao;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.folio.rest.jaxrs.model.DataImportQueueItemCollection;
import org.folio.service.processing.ranking.AbstractQueueItemRankerTest;
import org.folio.service.processing.ranking.QueueItemAgeRanker;
import org.folio.service.processing.ranking.QueueItemHolisticRanker;
import org.folio.service.processing.ranking.QueueItemPartNumberRanker;
import org.folio.service.processing.ranking.QueueItemSizeRanker;
import org.folio.service.processing.ranking.QueueItemTenantUsageRanker;
import org.folio.service.processing.ranking.ScoreService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

/**
 * Combines all rankers with realistic values and properties
 */
@ExtendWith(MockitoExtension.class)
@TestInstance(Lifecycle.PER_CLASS)
public abstract class AbstractEndToEndRankingTest
  extends AbstractQueueItemRankerTest {

  @Mock
  DataImportQueueItemDao queueItemDao;

  QueueItemAgeRanker ageRanker;
  QueueItemPartNumberRanker partNumberRanker;
  QueueItemSizeRanker sizeRanker;
  QueueItemTenantUsageRanker tenantUsageRanker;
  QueueItemHolisticRanker ranker;

  ScoreService service;

  protected List<DataImportQueueItem> waiting = new ArrayList<>();
  protected List<DataImportQueueItem> inProgress = new ArrayList<>();
  protected List<DataImportQueueItem> expected = new ArrayList<>();

  long lastIdBit = 0;

  @BeforeEach
  public void setUpRanking() {
    ageRanker = new QueueItemAgeRanker(0, 150, 4320, 10000);
    partNumberRanker = new QueueItemPartNumberRanker(1, 0, 100);
    sizeRanker = new QueueItemSizeRanker(40, -40, 100000);
    tenantUsageRanker = new QueueItemTenantUsageRanker(100, -200);

    ranker =
      new QueueItemHolisticRanker(
        ageRanker,
        partNumberRanker,
        sizeRanker,
        tenantUsageRanker
      );

    service = new ScoreService(ranker, queueItemDao);

    this.initializeData();
  }

  protected abstract void initializeData();

  protected DataImportQueueItem item(
    String tenant,
    int size,
    int ageMinutes,
    int partNumber
  ) {
    // increment for next one
    lastIdBit += 1;

    return new DataImportQueueItem()
      .withId(new UUID(0, lastIdBit).toString())
      .withTenant(tenant)
      .withOriginalSize(size)
      .withTimestamp(
        Date.from(Instant.now().minus(ageMinutes, ChronoUnit.MINUTES))
      )
      .withPartNumber(partNumber);
  }

  protected DataImportQueueItemCollection collection(
    List<DataImportQueueItem> items
  ) {
    return new DataImportQueueItemCollection().withDataImportQueueItems(items);
  }

  @Test
  @DisplayName("should return items in expected order when all ranking factors are applied")
  void shouldReturnItemsInExpectedOrder() {
    // arrange
    NavigableSet<DataImportQueueItem> result = service.getRankedQueueItems(
      collection(inProgress),
      collection(waiting)
    );

    // assert
    assertThat(result).hasSize(expected.size());

    Iterator<DataImportQueueItem> actualIt = result.iterator();
    for (int i = 0; i < expected.size() && actualIt.hasNext(); i++) {
      assertThat(actualIt.next()).isEqualTo(expected.get(i));
    }
  }
}
