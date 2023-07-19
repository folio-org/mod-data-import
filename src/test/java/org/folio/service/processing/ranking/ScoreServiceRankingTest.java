package org.folio.service.processing.ranking;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.util.Arrays;
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
    return new DataImportQueueItem().withTenant(tenant);
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

  private void mockDatabaseContents(
    DataImportQueueItemCollection waiting,
    DataImportQueueItemCollection inProgress
  ) {
    when(queueItemDao.getAllInProgressQueueItems())
      .thenReturn(Future.succeededFuture(inProgress));
    when(queueItemDao.getAllWaitingQueueItems())
      .thenReturn(Future.succeededFuture(waiting));
  }

  @Test
  public void testEmpty(TestContext context) {
    Async async = context.async();

    DataImportQueueItemCollection waiting = collectionOfTenant();
    DataImportQueueItemCollection inProgress = collectionOfTenant();
    mockDatabaseContents(waiting, inProgress);

    service
      .getRankedQueueItems()
      .onFailure(cause -> context.fail(cause))
      .onSuccess(result ->
        context.verify(v -> {
          assertThat(result, is(empty()));

          verifyNoInteractions(ranker);

          async.complete();
        })
      );
  }

  @Test
  public void testEmptyWithInProgress(TestContext context) {
    Async async = context.async();

    DataImportQueueItemCollection waiting = collectionOfTenant();
    DataImportQueueItemCollection inProgress = collectionOfTenant("A", "B");
    mockDatabaseContents(waiting, inProgress);

    service
      .getRankedQueueItems()
      .onFailure(cause -> context.fail(cause))
      .onSuccess(result ->
        context.verify(v -> {
          assertThat(result, is(empty()));

          verifyNoInteractions(ranker);

          async.complete();
        })
      );
  }

  @Test
  public void testSingleton(TestContext context) {
    Async async = context.async();

    DataImportQueueItemCollection waiting = collectionOfTenant("A");
    DataImportQueueItemCollection inProgress = collectionOfTenant();
    mockDatabaseContents(waiting, inProgress);

    service
      .getRankedQueueItems()
      .onFailure(cause -> context.fail(cause))
      .onSuccess(result ->
        context.verify(v -> {
          assertThat(
            result,
            contains(waiting.getDataImportQueueItems().get(0))
          );

          waiting
            .getDataImportQueueItems()
            .forEach(item -> verify(ranker, times(1)).score(eq(item), any()));
          verifyNoMoreInteractions(ranker);

          async.complete();
        })
      );
  }

  @Test
  public void testSingletonWithInProgress(TestContext context) {
    Async async = context.async();

    DataImportQueueItemCollection waiting = collectionOfTenant("A");
    DataImportQueueItemCollection inProgress = collectionOfTenant("B", "C");
    mockDatabaseContents(waiting, inProgress);

    service
      .getRankedQueueItems()
      .onFailure(cause -> context.fail(cause))
      .onSuccess(result ->
        context.verify(v -> {
          assertThat(
            result,
            contains(waiting.getDataImportQueueItems().get(0))
          );

          waiting
            .getDataImportQueueItems()
            .forEach(item -> verify(ranker, times(1)).score(eq(item), any()));
          verifyNoMoreInteractions(ranker);

          async.complete();
        })
      );
  }

  @Test
  public void testManyCase1(TestContext context) {
    Async async = context.async();

    DataImportQueueItemCollection waiting = collectionOfTenant("A", "B", "C");
    DataImportQueueItemCollection inProgress = collectionOfTenant("A", "B");
    mockDatabaseContents(waiting, inProgress);

    when(ranker.score(any(), any()))
      .thenAnswer(invocation -> {
        DataImportQueueItem item = invocation.getArgument(0);
        return Double.valueOf(item.getTenant().charAt(0));
      });

    service
      .getRankedQueueItems()
      .onFailure(cause -> context.fail(cause))
      .onSuccess(result ->
        context.verify(v -> {
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

          async.complete();
        })
      );
  }

  @Test
  public void testManyCase2(TestContext context) {
    Async async = context.async();

    DataImportQueueItemCollection waiting = collectionOfTenant("C", "B", "A");
    DataImportQueueItemCollection inProgress = collectionOfTenant("D", "B");
    mockDatabaseContents(waiting, inProgress);

    when(ranker.score(any(), any()))
      .thenAnswer(invocation -> {
        DataImportQueueItem item = invocation.getArgument(0);
        return Double.valueOf(item.getTenant().charAt(0));
      });

    service
      .getRankedQueueItems()
      .onFailure(cause -> context.fail(cause))
      .onSuccess(result ->
        context.verify(v -> {
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

          async.complete();
        })
      );
  }

  @Test
  public void testManyCase3(TestContext context) {
    Async async = context.async();

    DataImportQueueItemCollection waiting = collectionOfTenant("A", "C", "B");
    DataImportQueueItemCollection inProgress = collectionOfTenant("D", "B");
    mockDatabaseContents(waiting, inProgress);

    when(ranker.score(any(), any()))
      .thenAnswer(invocation -> {
        DataImportQueueItem item = invocation.getArgument(0);
        return Double.valueOf(item.getTenant().charAt(0));
      });

    service
      .getRankedQueueItems()
      .onFailure(cause -> context.fail(cause))
      .onSuccess(result ->
        context.verify(v -> {
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

          async.complete();
        })
      );
  }

  @Test
  public void testGetBest(TestContext context) {
    Async async = context.async();

    DataImportQueueItemCollection waiting = collectionOfTenant("A", "C", "B");
    DataImportQueueItemCollection inProgress = collectionOfTenant("D", "B");
    mockDatabaseContents(waiting, inProgress);

    when(ranker.score(any(), any()))
      .thenAnswer(invocation -> {
        DataImportQueueItem item = invocation.getArgument(0);
        return Double.valueOf(item.getTenant().charAt(0));
      });

    service
      .getBestQueueItem()
      .onFailure(cause -> context.fail(cause))
      .onSuccess(result ->
        context.verify(v -> {
          assertThat(
            result.orElseThrow(),
            is(
              equalTo(
                // C should come first because it has the highest car code
                waiting.getDataImportQueueItems().get(1)
              )
            )
          );

          waiting
            .getDataImportQueueItems()
            .forEach(item -> verify(ranker, times(1)).score(eq(item), any()));
          verifyNoMoreInteractions(ranker);

          async.complete();
        })
      );
  }

  @Test
  public void testGetBestEmpty(TestContext context) {
    Async async = context.async();

    DataImportQueueItemCollection waiting = collectionOfTenant();
    DataImportQueueItemCollection inProgress = collectionOfTenant("D", "B");
    mockDatabaseContents(waiting, inProgress);

    when(ranker.score(any(), any()))
      .thenAnswer(invocation -> {
        DataImportQueueItem item = invocation.getArgument(0);
        return Double.valueOf(item.getTenant().charAt(0));
      });

    service
      .getBestQueueItem()
      .onFailure(cause -> context.fail(cause))
      .onSuccess(result ->
        context.verify(v -> {
          assertThat(result.isEmpty(), is(true));

          waiting
            .getDataImportQueueItems()
            .forEach(item -> verify(ranker, times(1)).score(eq(item), any()));
          verifyNoMoreInteractions(ranker);

          async.complete();
        })
      );
  }
}
