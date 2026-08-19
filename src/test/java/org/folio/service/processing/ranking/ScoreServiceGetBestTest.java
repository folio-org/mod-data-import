package org.folio.service.processing.ranking;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.util.Arrays;
import java.util.Optional;
import java.util.UUID;
import java.util.function.BiFunction;
import org.folio.dao.DataImportQueueItemDao;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.folio.rest.jaxrs.model.DataImportQueueItemCollection;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class ScoreServiceGetBestTest {

  @Mock
  DataImportQueueItemDao queueItemDao;

  @Mock
  QueueItemHolisticRanker ranker;

  @InjectMocks
  ScoreService service;

  private DataImportQueueItem ofTenant(String tenant) {
    return new DataImportQueueItem()
      .withId(new UUID(0, 0).toString())
      .withTenant(tenant);
  }

  private DataImportQueueItemCollection collection(DataImportQueueItem... items) {
    return new DataImportQueueItemCollection()
      .withDataImportQueueItems(Arrays.asList(items));
  }

  private DataImportQueueItemCollection collectionOfTenant(String... tenants) {
    return collection(
      Arrays.stream(tenants)
        .map(this::ofTenant)
        .toArray(DataImportQueueItem[]::new)
    );
  }

  // casting with generics makes it sad :(
  private void mockDatabaseContents(
    DataImportQueueItemCollection waiting,
    DataImportQueueItemCollection inProgress
  ) {
    when(queueItemDao.getAllQueueItemsAndProcessAtomic(any()))
      .thenAnswer(invocation -> {
        BiFunction<DataImportQueueItemCollection, DataImportQueueItemCollection, Optional<DataImportQueueItem>> processor =
          invocation.getArgument(0);
        return Future.succeededFuture(processor.apply(inProgress, waiting));
      });
  }

  @DisplayName("should return item with highest score when multiple items are waiting")
  @Test
  void shouldReturnItemWithHighestScore_whenMultipleItemsAreWaiting(VertxTestContext testContext) {
    DataImportQueueItemCollection waiting = collectionOfTenant("A", "C", "B");
    DataImportQueueItemCollection inProgress = collectionOfTenant("D", "B");
    mockDatabaseContents(waiting, inProgress);

    when(ranker.score(any(), any()))
      .thenAnswer(invocation -> {
        DataImportQueueItem item = invocation.getArgument(0);
        return (double) item.getTenant().charAt(0);
      });

    service.getBestQueueItemAndMarkInProgress()
      .onComplete(testContext.succeeding(result -> testContext.verify(() -> {
        // C should come first because it has the highest char code
        assertThat(result.orElseThrow()).isEqualTo(waiting.getDataImportQueueItems().get(1));

        waiting.getDataImportQueueItems()
          .forEach(item -> verify(ranker, times(1)).score(eq(item), any()));
        verifyNoMoreInteractions(ranker);

        testContext.completeNow();
      })));
  }

  @DisplayName("should return empty optional when no items are waiting")
  @Test
  void shouldReturnEmpty_whenNoItemsAreWaiting(VertxTestContext testContext) {
    DataImportQueueItemCollection waiting = collectionOfTenant();
    DataImportQueueItemCollection inProgress = collectionOfTenant("D", "B");
    mockDatabaseContents(waiting, inProgress);

    service.getBestQueueItemAndMarkInProgress()
      .onComplete(testContext.succeeding(result -> testContext.verify(() -> {
        assertThat(result).isEmpty();

        waiting.getDataImportQueueItems()
          .forEach(item -> verify(ranker, times(1)).score(eq(item), any()));
        verifyNoMoreInteractions(ranker);

        testContext.completeNow();
      })));
  }
}
