package org.folio.service.processing.ranking;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.NavigableSet;
import java.util.UUID;
import org.folio.dao.DataImportQueueItemDao;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.folio.rest.jaxrs.model.DataImportQueueItemCollection;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ScoreServiceRankingTest {

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

  @DisplayName("should return empty set when waiting queue is empty")
  @Test
  void shouldReturnEmptySet_whenWaitingQueueIsEmpty() {
    // arrange
    DataImportQueueItemCollection waiting = collectionOfTenant();
    DataImportQueueItemCollection inProgress = collectionOfTenant();

    // act
    NavigableSet<DataImportQueueItem> result = service.getRankedQueueItems(
      inProgress,
      waiting
    );

    // assert
    assertThat(result).isEmpty();
    verifyNoInteractions(ranker);
  }

  @DisplayName("should return empty set when waiting queue is empty and items are in progress")
  @Test
  void shouldReturnEmptySet_whenWaitingQueueIsEmptyWithInProgress() {
    // arrange
    DataImportQueueItemCollection waiting = collectionOfTenant();
    DataImportQueueItemCollection inProgress = collectionOfTenant("A", "B");

    // act
    NavigableSet<DataImportQueueItem> result = service.getRankedQueueItems(
      inProgress,
      waiting
    );

    // assert
    assertThat(result).isEmpty();
    verifyNoInteractions(ranker);
  }

  @DisplayName("should return single item when waiting queue has one item")
  @Test
  void shouldReturnSingleItem_whenWaitingQueueHasSingleItem() {
    // arrange
    DataImportQueueItemCollection waiting = collectionOfTenant("A");
    DataImportQueueItemCollection inProgress = collectionOfTenant();

    // act
    NavigableSet<DataImportQueueItem> result = service.getRankedQueueItems(
      inProgress,
      waiting
    );

    // assert
    assertThat(result).containsExactly(waiting.getDataImportQueueItems().getFirst());

    waiting
      .getDataImportQueueItems()
      .forEach(item -> verify(ranker, times(1)).score(eq(item), any()));
    verifyNoMoreInteractions(ranker);
  }

  @DisplayName("should return single item when waiting queue has one item and others are in progress")
  @Test
  void shouldReturnSingleItem_whenWaitingQueueHasSingleItemWithInProgress() {
    // arrange
    DataImportQueueItemCollection waiting = collectionOfTenant("A");
    DataImportQueueItemCollection inProgress = collectionOfTenant("B", "C");

    // act
    NavigableSet<DataImportQueueItem> result = service.getRankedQueueItems(
      inProgress,
      waiting
    );

    // assert
    assertThat(result).containsExactly(waiting.getDataImportQueueItems().getFirst());

    waiting
      .getDataImportQueueItems()
      .forEach(item -> verify(ranker, times(1)).score(eq(item), any()));
    verifyNoMoreInteractions(ranker);
  }

  @DisplayName("should rank items by score descending when tenants are A, B, C with A and B in progress")
  @Test
  void shouldRankItemsByScoreDescending_whenTenantsAreABCWithABInProgress() {
    // arrange
    DataImportQueueItemCollection waiting = collectionOfTenant("A", "B", "C");
    DataImportQueueItemCollection inProgress = collectionOfTenant("A", "B");

    when(ranker.score(any(), any()))
      .thenAnswer(invocation -> {
        DataImportQueueItem item = invocation.getArgument(0);
        return (double) item.getTenant().charAt(0);
      });

    // act
    NavigableSet<DataImportQueueItem> result = service.getRankedQueueItems(
      inProgress,
      waiting
    );

    // assert
    assertThat(result).containsExactly(
      // C should come first because it has the highest char code
      waiting.getDataImportQueueItems().get(2),
      waiting.getDataImportQueueItems().get(1),
      waiting.getDataImportQueueItems().get(0)
    );

    waiting
      .getDataImportQueueItems()
      .forEach(item -> verify(ranker, times(1)).score(eq(item), any()));
    verifyNoMoreInteractions(ranker);
  }

  @DisplayName("should rank items by score descending when tenants are C, B, A with D and B in progress")
  @Test
  void shouldRankItemsByScoreDescending_whenTenantsAreCBAWithDBInProgress() {
    // arrange
    DataImportQueueItemCollection waiting = collectionOfTenant("C", "B", "A");
    DataImportQueueItemCollection inProgress = collectionOfTenant("D", "B");

    when(ranker.score(any(), any()))
      .thenAnswer(invocation -> {
        DataImportQueueItem item = invocation.getArgument(0);
        return (double) item.getTenant().charAt(0);
      });

    // act
    NavigableSet<DataImportQueueItem> result = service.getRankedQueueItems(
      inProgress,
      waiting
    );

    // assert
    assertThat(result).containsExactly(
      // C should come first because it has the highest char code
      waiting.getDataImportQueueItems().get(0),
      waiting.getDataImportQueueItems().get(1),
      waiting.getDataImportQueueItems().get(2)
    );

    waiting
      .getDataImportQueueItems()
      .forEach(item -> verify(ranker, times(1)).score(eq(item), any()));
    verifyNoMoreInteractions(ranker);
  }

  @DisplayName("should rank items by score descending when tenants are A, C, B with D and B in progress")
  @Test
  void shouldRankItemsByScoreDescending_whenTenantsAreACBWithDBInProgress() {
    // arrange
    DataImportQueueItemCollection waiting = collectionOfTenant("A", "C", "B");
    DataImportQueueItemCollection inProgress = collectionOfTenant("D", "B");

    when(ranker.score(any(), any()))
      .thenAnswer(invocation -> {
        DataImportQueueItem item = invocation.getArgument(0);
        return (double) item.getTenant().charAt(0);
      });

    // act
    NavigableSet<DataImportQueueItem> result = service.getRankedQueueItems(
      inProgress,
      waiting
    );

    // assert
    assertThat(result).containsExactly(
      // C should come first because it has the highest char code
      waiting.getDataImportQueueItems().get(1),
      waiting.getDataImportQueueItems().get(2),
      waiting.getDataImportQueueItems().get(0)
    );

    waiting
      .getDataImportQueueItems()
      .forEach(item -> verify(ranker, times(1)).score(eq(item), any()));
    verifyNoMoreInteractions(ranker);
  }
}
