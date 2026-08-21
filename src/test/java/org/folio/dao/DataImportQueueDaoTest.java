package org.folio.dao;

import static java.time.Month.FEBRUARY;
import static java.time.Month.JANUARY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import io.vertx.junit5.VertxTestContext;
import io.vertx.pgclient.PgConnection;
import io.vertx.sqlclient.Tuple;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import lombok.extern.log4j.Log4j2;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.folio.rest.jaxrs.model.DataImportQueueItemCollection;
import org.folio.rest.persist.PostgresClient;
import org.folio.support.AbstractRestTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@Log4j2
class DataImportQueueDaoTest extends AbstractRestTest {

  private DataImportQueueItem waiting1;
  private DataImportQueueItem waiting2;
  private DataImportQueueItem inProgress1;
  private DataImportQueueItem inProgress2;

  private DataImportQueueItemDao queueItemDao;

  @BeforeEach
  void initializeVariables() {
    waiting1 = new DataImportQueueItem()
      .withId("922fdc41-7a55-5269-a8ff-56038f18c477")
      .withJobExecutionId("5dd15bc5-373b-5d00-b7db-2cf094e9a785")
      .withUploadDefinitionId("aca5103a-e602-5201-8a55-8dcee9bfbb85")
      .withTenant("tenant-1").withOriginalSize(111).withFilePath("file-path-1")
      .withTimestamp(Date.from(LocalDateTime.of(2021, JANUARY, 1, 1, 1).toInstant(ZoneOffset.UTC)))
      .withPartNumber(1).withProcessing(false).withOkapiUrl("okapi-url-1").withDataType("data-type-1")
      .withOkapiToken("okapi-token-1").withOkapiPermissions("okapi-permissions-1")
      .withOkapiRequestId("okapi-request-id-1");
    waiting2 = new DataImportQueueItem()
      .withId("c7e786c6-6fd1-5633-9d09-ced97fc1ff9e")
      .withJobExecutionId("5dd15bc5-373b-5d00-b7db-2cf094e9a785")
      .withUploadDefinitionId("0dec7b87-8ebe-5cd0-894c-7e7a58974fe5")
      .withTenant("tenant-2").withOriginalSize(222).withFilePath("file-path-2")
      .withTimestamp(Date.from(LocalDateTime.of(2022, FEBRUARY, 2, 2, 2).toInstant(ZoneOffset.UTC)))
      .withPartNumber(2).withProcessing(false).withOkapiUrl("okapi-url-2").withDataType("data-type-2")
      .withOkapiToken("okapi-token-2").withOkapiPermissions("okapi-permissions-2")
      .withOkapiRequestId("okapi-request-id-2");
    inProgress1 = new DataImportQueueItem()
      .withId("15c40f1e-d685-57a0-901a-c68980c9c50b")
      .withJobExecutionId("9ff47a6e-8133-5585-af9a-2f8f204a2726")
      .withUploadDefinitionId("bb08fdfe-1b12-5a26-9fb7-02218eb6c32c")
      .withTenant("tenant-p-1").withOriginalSize(1011).withFilePath("file-path-p-1")
      .withTimestamp(Date.from(LocalDateTime.of(2121, JANUARY, 1, 1, 1).toInstant(ZoneOffset.UTC)))
      .withPartNumber(1).withProcessing(true).withOkapiUrl("okapi-url-p-1").withDataType("data-type-p-1")
      .withOkapiToken("okapi-token-p-1").withOkapiPermissions("okapi-permissions-p-1")
      .withOkapiRequestId("okapi-request-id-p-1");
    inProgress2 = new DataImportQueueItem()
      .withId("9a197f42-16c9-5a4b-a213-1adf4498fb02")
      .withJobExecutionId("0532592a-ba10-5a7e-bb2b-aea0b4aeb459")
      .withUploadDefinitionId("fbf6a813-9462-5be8-9078-4b7b5ad44065")
      .withTenant("tenant-p-2").withOriginalSize(2022).withFilePath("file-path-p-2")
      .withTimestamp(Date.from(LocalDateTime.of(2122, FEBRUARY, 2, 2, 2).toInstant(ZoneOffset.UTC)))
      .withPartNumber(2).withProcessing(true).withOkapiUrl("okapi-url-p-2").withDataType("data-type-p-2")
      .withOkapiToken("okapi-token-p-2").withOkapiPermissions("okapi-permissions-p-2")
      .withOkapiRequestId("okapi-request-id-p-2");

    queueItemDao = new DataImportQueueItemDaoImpl(new PostgresClientFactory(vertx));
  }

  @DisplayName("should return empty collection when no items exist")
  @Test
  void shouldReturnEmptyCollection_whenNoItemsExist(VertxTestContext testContext) {
    queueItemDao.getAllQueueItems().onComplete(
      testContext.succeeding(x -> testContext.verify(() -> {
        assertThat(x.getTotalRecords()).isZero();
        assertThat(x.getDataImportQueueItems()).isEmpty();
        testContext.completeNow();
      }))
    );
  }

  @DisplayName("should return all queue items when items have been added")
  @Test
  void shouldReturnAllQueueItems_whenItemsHaveBeenAdded(VertxTestContext testContext) {
    Future.all(
        queueItemDao.addQueueItem(waiting1),
        queueItemDao.addQueueItem(waiting2),
        queueItemDao.addQueueItem(inProgress1),
        queueItemDao.addQueueItem(inProgress2)
      ).compose(v -> queueItemDao.getAllQueueItems())
      .onComplete(testContext.succeeding(result -> testContext.verify(() -> {
        assertThat(result.getTotalRecords()).isEqualTo(4);
        assertThat(result.getDataImportQueueItems())
          .containsExactlyInAnyOrder(waiting1, waiting2, inProgress1, inProgress2);
        testContext.completeNow();
      })));
  }

  @DisplayName("should return waiting and in-progress items separately via connection")
  @Test
  void shouldReturnWaitingAndInProgressItemsSeparately(VertxTestContext testContext) {
    Future.all(
        queueItemDao.addQueueItem(waiting1),
        queueItemDao.addQueueItem(waiting2),
        queueItemDao.addQueueItem(inProgress1),
        queueItemDao.addQueueItem(inProgress2)
      ).compose(v -> PostgresClient.getInstance(vertx).getConnection())
      .compose(connection -> Future.all(
        queueItemDao.getAllWaitingQueueItems(connection),
        queueItemDao.getAllInProgressQueueItems(connection)
      )).onComplete(testContext.succeeding(cf -> testContext.verify(() -> {
        DataImportQueueItemCollection waiting = cf.resultAt(0);
        DataImportQueueItemCollection inProgress = cf.resultAt(1);
        assertThat(waiting.getTotalRecords()).isEqualTo(2);
        assertThat(inProgress.getTotalRecords()).isEqualTo(2);
        assertThat(waiting.getDataImportQueueItems()).containsExactlyInAnyOrder(waiting1, waiting2);
        assertThat(inProgress.getDataImportQueueItems()).containsExactlyInAnyOrder(inProgress1, inProgress2);
        testContext.completeNow();
      })));
  }

  @DisplayName("should not change items when atomic update returns empty optional")
  @Test
  void shouldNotChangeItems_whenAtomicUpdateReturnsEmpty(VertxTestContext testContext) {
    Future.all(
      queueItemDao.addQueueItem(waiting1),
      queueItemDao.addQueueItem(waiting2),
      queueItemDao.addQueueItem(inProgress1),
      queueItemDao.addQueueItem(inProgress2)
    ).compose(v -> queueItemDao.getAllQueueItemsAndProcessAtomic((inProgress, waiting) -> {
      assertThat(inProgress.getDataImportQueueItems()).containsExactlyInAnyOrder(inProgress1, inProgress2);
      assertThat(waiting.getDataImportQueueItems()).containsExactlyInAnyOrder(waiting1, waiting2);
      return Optional.empty();
    })).compose(result -> {
      assertThat(result).isEmpty();
      return queueItemDao.getAllQueueItems();
    }).onComplete(testContext.succeeding(r -> testContext.verify(() -> {
      assertThat(r.getDataImportQueueItems()).containsExactlyInAnyOrder(waiting1, waiting2, inProgress1, inProgress2);
      testContext.completeNow();
    })));
  }

  @DisplayName("should mark item as processing when atomic update returns item")
  @Test
  void shouldMarkItemAsProcessing_whenAtomicUpdateReturnsItem(VertxTestContext testContext) {
    Future.all(
      queueItemDao.addQueueItem(waiting1),
      queueItemDao.addQueueItem(waiting2),
      queueItemDao.addQueueItem(inProgress1),
      queueItemDao.addQueueItem(inProgress2)
    ).compose(v -> queueItemDao.getAllQueueItemsAndProcessAtomic((inProgress, waiting) -> {
      assertThat(inProgress.getDataImportQueueItems()).containsExactlyInAnyOrder(inProgress1, inProgress2);
      assertThat(waiting.getDataImportQueueItems()).containsExactlyInAnyOrder(waiting1, waiting2);
      return Optional.of(waiting1);
    })).compose(result -> {
      assertThat(result).contains(waiting1);
      return queueItemDao.getAllQueueItems();
    }).onComplete(testContext.succeeding(r -> testContext.verify(() -> {
      assertThat(r.getDataImportQueueItems())
        .containsExactlyInAnyOrder(waiting1.withProcessing(true), waiting2, inProgress1, inProgress2);
      testContext.completeNow();
    })));
  }

  @DisplayName("should allow only one worker to claim the same queue item atomically")
  @Test
  void shouldAllowOnlyOneWorker_toClaimSameQueueItemAtomically(VertxTestContext testContext) {
    // arrange
    Future.all(queueItemDao.addQueueItem(waiting1), queueItemDao.addQueueItem(waiting2))
      .compose(v -> {
        // act — both workers race; which one wins the ACCESS EXCLUSIVE lock is non-deterministic
        Future<Optional<DataImportQueueItem>> worker1Future =
          queueItemDao.getAllQueueItemsAndProcessAtomic((inProgress, waiting) ->
            waiting.getDataImportQueueItems().stream()
              .filter(item -> item.getId().equals(waiting1.getId()))
              .findFirst()
          );

        Future<Optional<DataImportQueueItem>> worker2Future =
          queueItemDao.getAllQueueItemsAndProcessAtomic((inProgress, waiting) ->
            waiting.getDataImportQueueItems().stream()
              .filter(item -> item.getId().equals(waiting1.getId()))
              .findFirst()
          );

        // assert — exactly one worker claims the item regardless of lock ordering
        return Future.all(worker1Future, worker2Future)
          .map(cf -> {
            Optional<DataImportQueueItem> w1 = cf.resultAt(0);
            Optional<DataImportQueueItem> w2 = cf.resultAt(1);
            assertThat(List.of(w1, w2))
              .satisfiesExactlyInAnyOrder(
                present -> assertThat(present).isPresent(),
                absent -> assertThat(absent).isEmpty()
              );
            return (Void) null;
          });
      }).onComplete(testContext.succeedingThenComplete());
  }

  @DisplayName("should return queue item by ID and fail for non-existent IDs")
  @Test
  void shouldReturnQueueItemById_andFailForNonExistentIds(VertxTestContext testContext) {
    Future.all(queueItemDao.addQueueItem(waiting1), queueItemDao.addQueueItem(inProgress1))
      .compose(v -> Future.all(
        queueItemDao.getQueueItemById(waiting1.getId()),
        queueItemDao.getQueueItemById(inProgress1.getId()),
        expectedFail(queueItemDao.getQueueItemById(waiting2.getId())),
        expectedFail(queueItemDao.getQueueItemById(inProgress2.getId()))
      )).onComplete(testContext.succeeding(cf -> testContext.verify(() -> {
        assertThat(cf.<DataImportQueueItem>resultAt(0)).isEqualTo(waiting1);
        assertThat(cf.<DataImportQueueItem>resultAt(1)).isEqualTo(inProgress1);
        testContext.completeNow();
      })));
  }

  @DisplayName("should update queue item and fail when item does not exist")
  @Test
  void shouldUpdateQueueItem_andFailWhenItemDoesNotExist(VertxTestContext testContext) {
    PostgresClient pgClient = PostgresClient.getInstance(vertx);
    Future.all(queueItemDao.addQueueItem(waiting1), queueItemDao.addQueueItem(inProgress1))
      .compose(v -> Future.all(
        pgClient.withConnection(connection -> queueItemDao.updateQueueItem(connection, waiting1)),
        expectedFail(pgClient.withConnection(connection -> queueItemDao.updateQueueItem(connection, waiting2)))
      )).onComplete(testContext.succeeding(cf -> testContext.verify(() -> {
        assertThat(cf.<DataImportQueueItem>resultAt(0)).isEqualTo(waiting1);
        testContext.completeNow();
      })));
  }

  @DisplayName("should delete queue item by ID and prevent double-delete")
  @Test
  void shouldDeleteQueueItemById_andPreventDoubleDelete(VertxTestContext testContext) {
    Future.all(queueItemDao.addQueueItem(waiting1), queueItemDao.addQueueItem(waiting2))
      .compose(v -> Future.all(
        expectedFail(queueItemDao.deleteQueueItemById(inProgress1.getId())),
        queueItemDao.deleteQueueItemById(waiting1.getId())
      )).compose(v -> Future.all(
        queueItemDao.getAllQueueItems()
          .map(remaining -> {
            assertThat(remaining.getDataImportQueueItems()).containsExactly(waiting2);
            return (Void) null;
          }),
        expectedFail(queueItemDao.deleteQueueItemById(waiting1.getId()))
      )).onComplete(testContext.succeedingThenComplete());
  }

  @DisplayName("should delete queue items by job execution ID and prevent double-delete")
  @Test
  void shouldDeleteQueueItemsByJobExecutionId_andPreventDoubleDelete(VertxTestContext testContext) {
    Future.all(
      queueItemDao.addQueueItem(waiting1),
      queueItemDao.addQueueItem(waiting2),
      queueItemDao.addQueueItem(inProgress1),
      queueItemDao.addQueueItem(inProgress2)
    ).compose(v -> Future.all(
      expectedFail(queueItemDao.deleteQueueItemsByJobExecutionId("0d8cac53-29ee-572b-b506-8bdb33f5331e")),
      queueItemDao.deleteQueueItemsByJobExecutionId(waiting1.getJobExecutionId())
    )).compose(cf -> {
      assertThat(cf.<Integer>resultAt(1)).isEqualTo(2);
      return Future.all(
        queueItemDao.getAllQueueItems()
          .map(remaining -> {
            assertThat(remaining.getDataImportQueueItems()).containsExactlyInAnyOrder(inProgress1, inProgress2);
            return (Void) null;
          }),
        expectedFail(queueItemDao.deleteQueueItemsByJobExecutionId(waiting1.getJobExecutionId()))
      );
    }).onComplete(testContext.succeedingThenComplete());
  }

  @DisplayName("should propagate exceptions from database when connection fails")
  @Test
  void shouldPropagateExceptions_whenDatabaseFails(VertxTestContext testContext) {
    PostgresClientFactory badPostgresFactory = mock(PostgresClientFactory.class);
    PostgresClient badPostgresClient = mock(PostgresClient.class);
    when(badPostgresFactory.getInstance()).thenReturn(badPostgresClient);
    doThrow(new RuntimeException("test exception")).when(badPostgresClient).select(any(), any());
    doThrow(new RuntimeException("test exception")).when(badPostgresClient).select(any(), any(Tuple.class), any());

    PgConnection badPgConnection = mock(PgConnection.class);
    doThrow(new RuntimeException("test exception")).when(badPgConnection).preparedQuery(anyString());

    DataImportQueueItemDao failingQueueItemDao = new DataImportQueueItemDaoImpl(badPostgresFactory);

    Future.all(
      expectedFail(failingQueueItemDao.getAllQueueItems()),
      expectedFail(failingQueueItemDao.getAllWaitingQueueItems(badPgConnection)),
      expectedFail(failingQueueItemDao.getAllInProgressQueueItems(badPgConnection)),
      expectedFail(failingQueueItemDao.getQueueItemById("test-id")),
      expectedFail(failingQueueItemDao.updateQueueItem(badPgConnection, waiting1))
    ).onComplete(testContext.succeedingThenComplete());
  }

  private static <T> Future<Void> expectedFail(Future<T> f) {
    return f.transform(ar -> ar.failed()
                             ? Future.succeededFuture()
                             : Future.failedFuture("Expected future to fail but it succeeded with: " + ar.result()));
  }
}
