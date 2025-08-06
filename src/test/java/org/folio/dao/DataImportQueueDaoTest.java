package org.folio.dao;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.Optional;

import io.vertx.pgclient.PgConnection;
import io.vertx.sqlclient.Tuple;
import lombok.extern.log4j.Log4j2;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.rest.AbstractRestTest;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.folio.rest.jaxrs.model.DataImportQueueItemCollection;
import org.folio.rest.persist.PostgresClient;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@Log4j2
@RunWith(VertxUnitRunner.class)
public class DataImportQueueDaoTest extends AbstractRestTest {

  private DataImportQueueItem WAITING_1;
  private DataImportQueueItem WAITING_2;
  private DataImportQueueItem IN_PROGRESS_1;
  private DataImportQueueItem IN_PROGRESS_2;

  private DataImportQueueItemDao queueItemDao;

  @Before
  public void initializeVariables() {
    WAITING_1 =
      new DataImportQueueItem()
        .withId("922fdc41-7a55-5269-a8ff-56038f18c477")
        .withJobExecutionId("5dd15bc5-373b-5d00-b7db-2cf094e9a785")
        .withUploadDefinitionId("aca5103a-e602-5201-8a55-8dcee9bfbb85")
        .withTenant("tenant-1")
        .withOriginalSize(111)
        .withFilePath("file-path-1")
        .withTimestamp(
          Date.from(
            LocalDateTime.of(2021, 1, 1, 1, 1).toInstant(ZoneOffset.UTC)
          )
        )
        .withPartNumber(1)
        .withProcessing(false)
        .withOkapiUrl("okapi-url-1")
        .withDataType("data-type-1");
    WAITING_2 =
      new DataImportQueueItem()
        .withId("c7e786c6-6fd1-5633-9d09-ced97fc1ff9e")
        .withJobExecutionId("5dd15bc5-373b-5d00-b7db-2cf094e9a785")
        .withUploadDefinitionId("0dec7b87-8ebe-5cd0-894c-7e7a58974fe5")
        .withTenant("tenant-2")
        .withOriginalSize(222)
        .withFilePath("file-path-2")
        .withTimestamp(
          Date.from(
            LocalDateTime.of(2022, 2, 2, 2, 2).toInstant(ZoneOffset.UTC)
          )
        )
        .withPartNumber(2)
        .withProcessing(false)
        .withOkapiUrl("okapi-url-2")
        .withDataType("data-type-2");

    IN_PROGRESS_1 =
      new DataImportQueueItem()
        .withId("15c40f1e-d685-57a0-901a-c68980c9c50b")
        .withJobExecutionId("9ff47a6e-8133-5585-af9a-2f8f204a2726")
        .withUploadDefinitionId("bb08fdfe-1b12-5a26-9fb7-02218eb6c32c")
        .withTenant("tenant-p-1")
        .withOriginalSize(1011)
        .withFilePath("file-path-p-1")
        .withTimestamp(
          Date.from(
            LocalDateTime.of(2121, 1, 1, 1, 1).toInstant(ZoneOffset.UTC)
          )
        )
        .withPartNumber(1)
        .withProcessing(true)
        .withOkapiUrl("okapi-url-p-1")
        .withDataType("data-type-p-1");
    IN_PROGRESS_2 =
      new DataImportQueueItem()
        .withId("9a197f42-16c9-5a4b-a213-1adf4498fb02")
        .withJobExecutionId("0532592a-ba10-5a7e-bb2b-aea0b4aeb459")
        .withUploadDefinitionId("fbf6a813-9462-5be8-9078-4b7b5ad44065")
        .withTenant("tenant-p-2")
        .withOriginalSize(2022)
        .withFilePath("file-path-p-2")
        .withTimestamp(
          Date.from(
            LocalDateTime.of(2122, 2, 2, 2, 2).toInstant(ZoneOffset.UTC)
          )
        )
        .withPartNumber(2)
        .withProcessing(true)
        .withOkapiUrl("okapi-url-p-2")
        .withDataType("data-type-p-2");

    queueItemDao =
      new DataImportQueueItemDaoImpl(new PostgresClientFactory(vertx));
  }

  @Test
  public void testGetAllQueueItemsEmpty(TestContext context) {
    queueItemDao
      .getAllQueueItems()
      .onComplete(
        context.asyncAssertSuccess(x -> {
          assertThat(x.getTotalRecords(), is(0));
          assertThat(x.getDataImportQueueItems(), hasSize(0));
        })
      );
  }

  @Test
  public void testGetAllQueueItems(TestContext context) {
    CompositeFuture
      .all(
        queueItemDao.addQueueItem(WAITING_1),
        queueItemDao.addQueueItem(WAITING_2),
        queueItemDao.addQueueItem(IN_PROGRESS_1),
        queueItemDao.addQueueItem(IN_PROGRESS_2)
      )
      .onComplete(
        context.asyncAssertSuccess(v ->
          queueItemDao
            .getAllQueueItems()
            .onComplete(
              context.asyncAssertSuccess(result -> {
                assertThat(result.getTotalRecords(), is(4));

                assertThat(
                  result.getDataImportQueueItems(),
                  containsInAnyOrder(
                    WAITING_1,
                    WAITING_2,
                    IN_PROGRESS_1,
                    IN_PROGRESS_2
                  )
                );
              })
            )
        )
      );
  }

  @Test
  public void testGetWaitingAndInProgress(TestContext context) {
    Future.all(
      queueItemDao.addQueueItem(WAITING_1),
      queueItemDao.addQueueItem(WAITING_2),
      queueItemDao.addQueueItem(IN_PROGRESS_1),
      queueItemDao.addQueueItem(IN_PROGRESS_2)
    )
    .compose(v -> PostgresClient.getInstance(vertx).getConnection())
    .compose(connection -> Future.all(
      queueItemDao.getAllWaitingQueueItems(connection),
      queueItemDao.getAllInProgressQueueItems(connection)
    ))
    .onComplete(context.asyncAssertSuccess(future -> {
      DataImportQueueItemCollection waitingQueueItems = future.resultAt(0);
      DataImportQueueItemCollection inProgressQueueItems = future.resultAt(1);
      assertThat(waitingQueueItems.getTotalRecords(), is(2));
      assertThat(inProgressQueueItems.getTotalRecords(), is(2));
      assertThat(waitingQueueItems.getDataImportQueueItems(), containsInAnyOrder(WAITING_1, WAITING_2));
      assertThat(inProgressQueueItems.getDataImportQueueItems(), containsInAnyOrder(IN_PROGRESS_1, IN_PROGRESS_2));
    }));
  }

  @Test
  public void testAtomicUpdateWithNoChange(TestContext context) {
    CompositeFuture
      .all(
        queueItemDao.addQueueItem(WAITING_1),
        queueItemDao.addQueueItem(WAITING_2),
        queueItemDao.addQueueItem(IN_PROGRESS_1),
        queueItemDao.addQueueItem(IN_PROGRESS_2)
      )
      .onComplete(
        context.asyncAssertSuccess(v ->
          queueItemDao
            .getAllQueueItemsAndProcessAtomic((inProgress, waiting) -> {
              context.verify(vv -> {
                assertThat(
                  inProgress.getDataImportQueueItems(),
                  containsInAnyOrder(IN_PROGRESS_1, IN_PROGRESS_2)
                );
                assertThat(
                  waiting.getDataImportQueueItems(),
                  containsInAnyOrder(WAITING_1, WAITING_2)
                );
              });

              return Optional.empty();
            })
            .onComplete(
              context.asyncAssertSuccess(result -> {
                assertThat(result.isEmpty(), is(true));

                // ensure that nothing changed
                queueItemDao
                  .getAllQueueItems()
                  .onComplete(
                    context.asyncAssertSuccess(r ->
                      assertThat(
                        r.getDataImportQueueItems(),
                        containsInAnyOrder(
                          WAITING_1,
                          WAITING_2,
                          IN_PROGRESS_1,
                          IN_PROGRESS_2
                        )
                      )
                    )
                  );
              })
            )
        )
      );
  }

  @Test
  public void testAtomicUpdateWithChange(TestContext context) {
    CompositeFuture
      .all(
        queueItemDao.addQueueItem(WAITING_1),
        queueItemDao.addQueueItem(WAITING_2),
        queueItemDao.addQueueItem(IN_PROGRESS_1),
        queueItemDao.addQueueItem(IN_PROGRESS_2)
      )
      .onComplete(
        context.asyncAssertSuccess(v ->
          queueItemDao
            .getAllQueueItemsAndProcessAtomic((inProgress, waiting) -> {
              context.verify(vv -> {
                assertThat(
                  inProgress.getDataImportQueueItems(),
                  containsInAnyOrder(IN_PROGRESS_1, IN_PROGRESS_2)
                );
                assertThat(
                  waiting.getDataImportQueueItems(),
                  containsInAnyOrder(WAITING_1, WAITING_2)
                );
              });

              return Optional.of(WAITING_1);
            })
            .onComplete(
              context.asyncAssertSuccess(result -> {
                assertThat(result.get(), is(WAITING_1));

                // ensure that WAITING_1 changed
                queueItemDao
                  .getAllQueueItems()
                  .onComplete(
                    context.asyncAssertSuccess(r ->
                      assertThat(
                        r.getDataImportQueueItems(),
                        containsInAnyOrder(
                          WAITING_1.withProcessing(true),
                          WAITING_2,
                          IN_PROGRESS_1,
                          IN_PROGRESS_2
                        )
                      )
                    )
                  );
              })
            )
        )
      );
  }

  @Test
  public void shouldAllowOnlyOneWorkerToProcessQueueItemAtSameTime(TestContext context) {
    Future.all(
      queueItemDao.addQueueItem(WAITING_1),
      queueItemDao.addQueueItem(WAITING_2)
    ).onComplete(context.asyncAssertSuccess(v -> {
      Future<Optional<DataImportQueueItem>> worker1Future =
        queueItemDao.getAllQueueItemsAndProcessAtomic((inProgress, waiting) -> {
          // Worker1 tries to process WAITING_1
          return waiting.getDataImportQueueItems().stream()
            .filter(item -> item.getId().equals(WAITING_1.getId()))
            .findFirst();
        });

      Future<Optional<DataImportQueueItem>> worker2Future =
        queueItemDao.getAllQueueItemsAndProcessAtomic((inProgress, waiting) -> {
          // Worker2 also tries to process WAITING_1
          Optional<DataImportQueueItem> itemOptional = waiting.getDataImportQueueItems().stream()
            .filter(item -> item.getId().equals(WAITING_1.getId()))
            .findFirst();
          context.assertTrue(itemOptional.isEmpty());
          return itemOptional;
        });

      Future.all(worker1Future, worker2Future).onComplete(context.asyncAssertSuccess(cf -> {
        assertTrue(worker1Future.result().isPresent());
        assertTrue(worker2Future.result().isEmpty());
      }));
    }));
  }

  @Test
  public void testGetById(TestContext context) {
    CompositeFuture
      .all(
        queueItemDao.addQueueItem(WAITING_1),
        queueItemDao.addQueueItem(IN_PROGRESS_1)
      )
      .onComplete(
        context.asyncAssertSuccess(v -> {
          queueItemDao
            .getQueueItemById(WAITING_1.getId())
            .onComplete(
              context.asyncAssertSuccess(result ->
                assertThat(result, is(WAITING_1))
              )
            );
          queueItemDao
            .getQueueItemById(IN_PROGRESS_1.getId())
            .onComplete(
              context.asyncAssertSuccess(result ->
                assertThat(result, is(IN_PROGRESS_1))
              )
            );

          queueItemDao
            .getQueueItemById(WAITING_2.getId())
            .onComplete(context.asyncAssertFailure());
          queueItemDao
            .getQueueItemById(IN_PROGRESS_2.getId())
            .onComplete(context.asyncAssertFailure());
        })
      );
  }

  @Test
  public void testUpdate(TestContext context) {
    PostgresClient pgClient = PostgresClient.getInstance(vertx);
    Future.all(queueItemDao.addQueueItem(WAITING_1), queueItemDao.addQueueItem(IN_PROGRESS_1))
      .onComplete(context.asyncAssertSuccess(v -> {
        pgClient.withConnection(connection -> queueItemDao.updateQueueItem(connection, WAITING_1))
          .onComplete(context.asyncAssertSuccess(result -> assertThat(result, is(WAITING_1))));

        // cannot update what does not exist
        pgClient.withConnection(connection -> queueItemDao.updateQueueItem(connection, WAITING_2))
          .onComplete(context.asyncAssertFailure());
      }));
  }

  @Test
  public void testDeleteById(TestContext context) {
    CompositeFuture
      .all(
        queueItemDao.addQueueItem(WAITING_1),
        queueItemDao.addQueueItem(WAITING_2)
      )
      .onComplete(
        context.asyncAssertSuccess(v -> {
          // cannot delete what was never there
          queueItemDao
            .deleteQueueItemById(IN_PROGRESS_1.getId())
            .onComplete(context.asyncAssertFailure());

          // successful deletion
          queueItemDao
            .deleteQueueItemById(WAITING_1.getId())
            .onComplete(
              context.asyncAssertSuccess(result -> {
                queueItemDao
                  .getAllQueueItems()
                  .onComplete(
                    context.asyncAssertSuccess(remaining ->
                      assertThat(
                        remaining.getDataImportQueueItems(),
                        contains(WAITING_2)
                      )
                    )
                  );

                // cannot delete twice
                queueItemDao
                  .deleteQueueItemById(WAITING_1.getId())
                  .onComplete(context.asyncAssertFailure());
              })
            );
        })
      );
  }

  @Test
  public void testDeleteByJobExecutionId(TestContext context) {
    CompositeFuture
      .all(
        queueItemDao.addQueueItem(WAITING_1),
        queueItemDao.addQueueItem(WAITING_2),
        queueItemDao.addQueueItem(IN_PROGRESS_1),
        queueItemDao.addQueueItem(IN_PROGRESS_2)
      )
      .onComplete(
        context.asyncAssertSuccess(v -> {
          // cannot delete what was never there
          queueItemDao
            .deleteQueueItemsByJobExecutionId(
              "0d8cac53-29ee-572b-b506-8bdb33f5331e"
            )
            .onComplete(context.asyncAssertFailure());

          // successful deletion
          queueItemDao
            .deleteQueueItemsByJobExecutionId(WAITING_1.getJobExecutionId())
            .onComplete(
              context.asyncAssertSuccess(numDeleted -> {
                assertThat(numDeleted, is(2));

                queueItemDao
                  .getAllQueueItems()
                  .onComplete(
                    context.asyncAssertSuccess(remaining ->
                      assertThat(
                        remaining.getDataImportQueueItems(),
                        containsInAnyOrder(IN_PROGRESS_1, IN_PROGRESS_2)
                      )
                    )
                  );

                // cannot delete twice
                queueItemDao
                  .deleteQueueItemsByJobExecutionId(
                    WAITING_1.getJobExecutionId()
                  )
                  .onComplete(context.asyncAssertFailure());
              })
            );
        })
      );
  }

  @Test
  public void testExceptional(TestContext context) {
    PostgresClientFactory badPostgresFactory = mock(PostgresClientFactory.class);
    PostgresClient badPostgresClient = mock(PostgresClient.class);
    when(badPostgresFactory.getInstance()).thenReturn(badPostgresClient);
    doThrow(new RuntimeException("test exception"))
      .when(badPostgresClient)
      .select(any(), any());
    doThrow(new RuntimeException("test exception"))
      .when(badPostgresClient)
      .select(any(), any(Tuple.class), any());

    PgConnection badPgConnection = mock(PgConnection.class);
    doThrow(new RuntimeException("test exception"))
      .when(badPgConnection)
      .preparedQuery(anyString());

    DataImportQueueItemDao failingQueueItemDao = new DataImportQueueItemDaoImpl(badPostgresFactory);

    failingQueueItemDao
      .getAllQueueItems()
      .onComplete(context.asyncAssertFailure());
    failingQueueItemDao
      .getAllWaitingQueueItems(badPgConnection)
      .onComplete(context.asyncAssertFailure());
    failingQueueItemDao
      .getAllInProgressQueueItems(badPgConnection)
      .onComplete(context.asyncAssertFailure());
    failingQueueItemDao
      .getQueueItemById("test-id")
      .onComplete(context.asyncAssertFailure());
    failingQueueItemDao
      .updateQueueItem(badPgConnection, WAITING_1)
      .onComplete(context.asyncAssertFailure());
  }

}
