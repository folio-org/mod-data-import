package org.folio.service.processing.ranking.e2e;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Combines all rankers with realistic values and properties
 */
@RunWith(VertxUnitRunner.class)
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

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);

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
        Instant.now().minus(ageMinutes, ChronoUnit.MINUTES).toString()
      )
      .withPartNumber(partNumber);
  }

  protected DataImportQueueItemCollection collection(
    List<DataImportQueueItem> items
  ) {
    return new DataImportQueueItemCollection().withDataImportQueueItems(items);
  }

  @Test
  public void testRanking() {
    NavigableSet<DataImportQueueItem> result = service.getRankedQueueItems(
      collection(inProgress),
      collection(waiting)
    );

    assertThat(result, hasSize(expected.size()));

    Iterator<DataImportQueueItem> actualIt = result.iterator();
    for (int i = 0; i < expected.size() && actualIt.hasNext(); i++) {
      assertThat(actualIt.next(), is(equalTo(expected.get(i))));
    }
  }
}
