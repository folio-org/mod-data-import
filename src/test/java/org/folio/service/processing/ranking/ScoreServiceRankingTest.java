package org.folio.service.processing.ranking;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.util.Arrays;
import java.util.NavigableSet;
import java.util.UUID;
import org.folio.dao.DataImportQueueItemDao;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.folio.rest.jaxrs.model.DataImportQueueItemCollection;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(VertxUnitRunner.class)
public class ScoreServiceRankingTest {

  @Mock
  DataImportQueueItemDao queueItemDao;

  @Mock
  QueueItemHolisticRanker ranker;

  @InjectMocks
  ScoreService service;

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);
  }

  private DataImportQueueItem ofTenant(String tenant) {
    return new DataImportQueueItem()
      .withId(new UUID(0, 0).toString())
      .withTenant(tenant);
  }

  private DataImportQueueItemCollection collection(
    DataImportQueueItem... items
  ) {
    return new DataImportQueueItemCollection()
      .withDataImportQueueItems(Arrays.asList(items));
  }

  private DataImportQueueItemCollection collectionOfTenant(String... items) {
    return collection(
      Arrays
        .stream(items)
        .map(this::ofTenant)
        .toArray(DataImportQueueItem[]::new)
    );
  }

  @Test
  public void testEmpty() {
    DataImportQueueItemCollection waiting = collectionOfTenant();
    DataImportQueueItemCollection inProgress = collectionOfTenant();

    NavigableSet<DataImportQueueItem> result = service.getRankedQueueItems(
      inProgress,
      waiting
    );

    assertThat(result, is(empty()));

    verifyNoInteractions(ranker);
  }

  @Test
  public void testEmptyWithInProgress() {
    DataImportQueueItemCollection waiting = collectionOfTenant();
    DataImportQueueItemCollection inProgress = collectionOfTenant("A", "B");

    NavigableSet<DataImportQueueItem> result = service.getRankedQueueItems(
      inProgress,
      waiting
    );

    assertThat(result, is(empty()));

    verifyNoInteractions(ranker);
  }

  @Test
  public void testSingleton() {
    DataImportQueueItemCollection waiting = collectionOfTenant("A");
    DataImportQueueItemCollection inProgress = collectionOfTenant();

    NavigableSet<DataImportQueueItem> result = service.getRankedQueueItems(
      inProgress,
      waiting
    );

    assertThat(result, contains(waiting.getDataImportQueueItems().get(0)));

    waiting
      .getDataImportQueueItems()
      .forEach(item -> verify(ranker, times(1)).score(eq(item), any()));
    verifyNoMoreInteractions(ranker);
  }

  @Test
  public void testSingletonWithInProgress() {
    DataImportQueueItemCollection waiting = collectionOfTenant("A");
    DataImportQueueItemCollection inProgress = collectionOfTenant("B", "C");

    NavigableSet<DataImportQueueItem> result = service.getRankedQueueItems(
      inProgress,
      waiting
    );

    assertThat(result, contains(waiting.getDataImportQueueItems().get(0)));

    waiting
      .getDataImportQueueItems()
      .forEach(item -> verify(ranker, times(1)).score(eq(item), any()));
    verifyNoMoreInteractions(ranker);
  }

  @Test
  public void testManyCase1() {
    DataImportQueueItemCollection waiting = collectionOfTenant("A", "B", "C");
    DataImportQueueItemCollection inProgress = collectionOfTenant("A", "B");

    when(ranker.score(any(), any()))
      .thenAnswer(invocation -> {
        DataImportQueueItem item = invocation.getArgument(0);
        return Double.valueOf(item.getTenant().charAt(0));
      });

    NavigableSet<DataImportQueueItem> result = service.getRankedQueueItems(
      inProgress,
      waiting
    );

    assertThat(
      result,
      contains(
        // C should come first because it has the highest car code
        waiting.getDataImportQueueItems().get(2),
        waiting.getDataImportQueueItems().get(1),
        waiting.getDataImportQueueItems().get(0)
      )
    );

    waiting
      .getDataImportQueueItems()
      .forEach(item -> verify(ranker, times(1)).score(eq(item), any()));
    verifyNoMoreInteractions(ranker);
  }

  @Test
  public void testManyCase2() {
    DataImportQueueItemCollection waiting = collectionOfTenant("C", "B", "A");
    DataImportQueueItemCollection inProgress = collectionOfTenant("D", "B");

    when(ranker.score(any(), any()))
      .thenAnswer(invocation -> {
        DataImportQueueItem item = invocation.getArgument(0);
        return Double.valueOf(item.getTenant().charAt(0));
      });

    NavigableSet<DataImportQueueItem> result = service.getRankedQueueItems(
      inProgress,
      waiting
    );

    assertThat(
      result,
      contains(
        // C should come first because it has the highest car code
        waiting.getDataImportQueueItems().get(0),
        waiting.getDataImportQueueItems().get(1),
        waiting.getDataImportQueueItems().get(2)
      )
    );

    waiting
      .getDataImportQueueItems()
      .forEach(item -> verify(ranker, times(1)).score(eq(item), any()));
    verifyNoMoreInteractions(ranker);
  }

  @Test
  public void testManyCase3() {
    DataImportQueueItemCollection waiting = collectionOfTenant("A", "C", "B");
    DataImportQueueItemCollection inProgress = collectionOfTenant("D", "B");

    when(ranker.score(any(), any()))
      .thenAnswer(invocation -> {
        DataImportQueueItem item = invocation.getArgument(0);
        return Double.valueOf(item.getTenant().charAt(0));
      });

    NavigableSet<DataImportQueueItem> result = service.getRankedQueueItems(
      inProgress,
      waiting
    );

    assertThat(
      result,
      contains(
        // C should come first because it has the highest car code
        waiting.getDataImportQueueItems().get(1),
        waiting.getDataImportQueueItems().get(2),
        waiting.getDataImportQueueItems().get(0)
      )
    );

    waiting
      .getDataImportQueueItems()
      .forEach(item -> verify(ranker, times(1)).score(eq(item), any()));
    verifyNoMoreInteractions(ranker);
  }
}
