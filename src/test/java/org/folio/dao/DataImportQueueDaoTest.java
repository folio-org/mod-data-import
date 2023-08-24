package org.folio.dao;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.pgclient.PgConnection;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import io.vertx.sqlclient.impl.ArrayTuple;
import java.util.Arrays;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Function;
import java.util.regex.Pattern;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.folio.rest.jaxrs.model.DataImportQueueItemCollection;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.helpers.LocalRowSet;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatchers;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.invocation.InvocationOnMock;

@RunWith(VertxUnitRunner.class)
public class DataImportQueueDaoTest {

  @Mock
  private PostgresClientFactory postgresClientFactory;

  @Mock
  private PostgresClient postgresClient;

  @Mock
  private PgConnection postgresConnection;

  @InjectMocks
  DataImportQueueItemDao queueItemDaoImpl = new DataImportQueueItemDaoImpl(
    postgresClientFactory
  );

  UUID storedItemUUID;

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    when(postgresClientFactory.getInstance()).thenReturn(postgresClient);
  }

  @Test
  public void testGetAllQueueItems(TestContext context) {
    // given
    doAnswer((InvocationOnMock invocation) -> {
        Promise<RowSet<Row>> promise = invocation.getArgument(1);
        promise.complete(new LocalRowSet(5));
        return null;
      })
      .when(postgresClient)
      .select(anyString(), ArgumentMatchers.<Promise<RowSet<Row>>>any());

    // when
    queueItemDaoImpl
      .getAllQueueItems()
      // then
      .onComplete(
        context.asyncAssertSuccess(x -> {
          verify(postgresClient, times(1))
            .select(
              startsWith("SELECT * FROM data_import_global.queue_items"),
              ArgumentMatchers.<Promise<RowSet<Row>>>any()
            );
          verifyNoMoreInteractions(postgresClient);
        })
      );
  }

  @Test
  public void testAddQueueItem(TestContext context) {
    // given
    doAnswer((InvocationOnMock invocation) -> {
        Promise<RowSet<Row>> promise = invocation.getArgument(2);
        promise.complete(new LocalRowSet(1));
        return null;
      })
      .when(postgresClient)
      .execute(
        anyString(),
        any(Tuple.class),
        ArgumentMatchers.<Promise<RowSet<Row>>>any()
      );

    // when
    DataImportQueueItem queueItem = new DataImportQueueItem();
    queueItem.setFilePath("test/file/path");
    storedItemUUID = UUID.randomUUID();
    queueItem.setId(storedItemUUID.toString());
    queueItem.setUploadDefinitionId(UUID.randomUUID().toString());
    queueItem.setJobExecutionId(UUID.randomUUID().toString());
    queueItem.setOriginalSize(5000);
    DateTime now = new DateTime();
    queueItem.setTimestamp(now.toDate());
    queueItemDaoImpl
      .addQueueItem(queueItem)
      // then
      .onComplete(
        context.asyncAssertSuccess(x -> {
          verify(postgresClient, times(1))
            .execute(
              startsWith("INSERT INTO data_import_global.queue_items "),
              any(Tuple.class),
              ArgumentMatchers.<Promise<RowSet<Row>>>any()
            );
          verifyNoMoreInteractions(postgresClient);
        })
      );
  }

  @Test
  public void testGetQueueItemByIdFailure(TestContext context) {
    Async async = context.async();

    // given
    doAnswer((InvocationOnMock invocation) -> {
        Promise<RowSet<Row>> promise = invocation.getArgument(2);
        promise.complete(new LocalRowSet(3));
        return null;
      })
      .when(postgresClient)
      .select(
        anyString(),
        any(Tuple.class),
        ArgumentMatchers.<Promise<RowSet<Row>>>any()
      );

    // when
    queueItemDaoImpl
      .getQueueItemById("sample-id")
      // then
      .onFailure(err -> {
        verify(postgresClient, times(1))
          .select(
            anyString(),
            any(Tuple.class),
            ArgumentMatchers.<Promise<RowSet<Row>>>any()
          );
        verifyNoMoreInteractions(postgresClient);

        assertThat(err, is(instanceOf(NoSuchElementException.class)));

        async.complete();
      })
      .onSuccess(er -> context.fail("Provided ID should not exist."));
  }

  @Test
  public void testGetWaitingQueueItems(TestContext context) {
    // given
    doAnswer((InvocationOnMock invocation) -> {
        Promise<RowSet<Row>> promise = invocation.getArgument(2);
        promise.complete(new LocalRowSet(3));
        ArrayTuple tuple = invocation.getArgument(1);
        assertThat(tuple.size(), is(1));
        assertThat(tuple.getBoolean(0), is(false));
        return null;
      })
      .when(postgresClient)
      .select(
        anyString(),
        any(Tuple.class),
        ArgumentMatchers.<Promise<RowSet<Row>>>any()
      );

    // when
    queueItemDaoImpl
      .getAllWaitingQueueItems()
      // then
      .onComplete(
        context.asyncAssertSuccess(x -> {
          verify(postgresClient, times(1))
            .select(
              startsWith(
                "SELECT * FROM data_import_global.queue_items WHERE processing = "
              ),
              any(Tuple.class),
              ArgumentMatchers.<Promise<RowSet<Row>>>any()
            );
          verifyNoMoreInteractions(postgresClient);
        })
      );
  }

  @Test
  public void testGetInProgressQueueItems(TestContext context) {
    // given
    doAnswer((InvocationOnMock invocation) -> {
        Promise<RowSet<Row>> promise = invocation.getArgument(2);
        promise.complete(new LocalRowSet(3));
        ArrayTuple tuple = invocation.getArgument(1);
        assertThat(tuple.size(), is(1));
        assertThat(tuple.getBoolean(0), is(true));
        return null;
      })
      .when(postgresClient)
      .select(
        anyString(),
        any(Tuple.class),
        ArgumentMatchers.<Promise<RowSet<Row>>>any()
      );

    // when
    queueItemDaoImpl
      .getAllInProgressQueueItems()
      // then
      .onComplete(
        context.asyncAssertSuccess(x -> {
          verify(postgresClient, times(1))
            .select(
              startsWith(
                "SELECT * FROM data_import_global.queue_items WHERE processing = "
              ),
              any(Tuple.class),
              ArgumentMatchers.<Promise<RowSet<Row>>>any()
            );
          verifyNoMoreInteractions(postgresClient);
        })
      );
  }

  // casting to generics makes it sad :(
  @SuppressWarnings("unchecked")
  @Test
  public void testAtomicGetAndUpdateEmpty(TestContext context) {
    DataImportQueueItemDao testInstance = spy(
      new DataImportQueueItemDaoImpl(postgresClientFactory)
    );

    DataImportQueueItem testWaiting1 = new DataImportQueueItem().withId("A");
    DataImportQueueItem testWaiting2 = new DataImportQueueItem().withId("B");
    DataImportQueueItem testInProgress = new DataImportQueueItem().withId("C");

    when(testInstance.getAllWaitingQueueItems())
      .thenReturn(
        Future.succeededFuture(
          new DataImportQueueItemCollection()
            .withDataImportQueueItems(Arrays.asList(testWaiting1, testWaiting2))
        )
      );
    when(testInstance.getAllInProgressQueueItems())
      .thenReturn(
        Future.succeededFuture(
          new DataImportQueueItemCollection()
            .withDataImportQueueItems(Arrays.asList(testInProgress))
        )
      );
    when(testInstance.getAllQueueItemsAndProcessAtomic(any()))
      .thenCallRealMethod();

    when(postgresClient.withTransaction(any()))
      .thenAnswer((InvocationOnMock invocation) ->
        (
          (Function<PgConnection, Future<Optional<DataImportQueueItem>>>) (
            invocation.getArgument(0)
          )
        ).apply(postgresConnection)
      );

    // response value unused
    when(postgresConnection.query(anyString())).thenReturn(null);

    testInstance
      .getAllQueueItemsAndProcessAtomic((inProgress, waiting) -> {
        assertThat(
          inProgress.getDataImportQueueItems(),
          contains(testInProgress)
        );
        assertThat(
          waiting.getDataImportQueueItems(),
          contains(testWaiting1, testWaiting2)
        );

        return Optional.empty();
      })
      .onComplete(
        context.asyncAssertSuccess(result -> {
          assertThat(result.isEmpty(), is(true));

          verify(postgresConnection, times(1))
            .query(
              matches(
                Pattern.compile(
                  "^LOCK TABLE \\w+\\.\\w+ IN ACCESS EXCLUSIVE MODE$"
                )
              )
            );

          verify(postgresClient, atLeastOnce()).withTransaction(any());
        })
      );
  }

  @Test
  public void testUpdateQueueItemById(TestContext context) {
    // given
    doAnswer((InvocationOnMock invocation) -> {
        Promise<RowSet<Row>> promise = invocation.getArgument(2);
        promise.complete(new LocalRowSet(1));
        return null;
      })
      .when(postgresClient)
      .execute(
        anyString(),
        any(Tuple.class),
        ArgumentMatchers.<Promise<RowSet<Row>>>any()
      );

    // when
    DataImportQueueItem queueItem = new DataImportQueueItem();
    queueItem.setFilePath("test/file/path");
    storedItemUUID = UUID.randomUUID();
    queueItem.setId(storedItemUUID.toString());
    queueItem.setUploadDefinitionId(UUID.randomUUID().toString());
    queueItem.setJobExecutionId(UUID.randomUUID().toString());
    queueItem.setOriginalSize(5000);
    DateTime now = new DateTime();
    queueItem.setTimestamp(now.toDate());
    queueItemDaoImpl
      .updateDataImportQueueItem(queueItem)
      // then
      .onComplete(
        context.asyncAssertSuccess(x -> {
          verify(postgresClient, times(1))
            .execute(
              startsWith("UPDATE data_import_global.queue_items SET"),
              any(Tuple.class),
              ArgumentMatchers.<Promise<RowSet<Row>>>any()
            );
          verifyNoMoreInteractions(postgresClient);
        })
      );
  }

  @Test
  public void testDeleteQueueItemById(TestContext context) {
    // given
    when(postgresClient.execute(anyString(), any(Tuple.class)))
      .thenReturn(Future.succeededFuture(new LocalRowSet(1)));

    // when
    queueItemDaoImpl
      .deleteDataImportQueueItem("sample-id")
      // then
      .onComplete(
        context.asyncAssertSuccess(x -> {
          verify(postgresClient, times(1))
            .execute(
              eq("DELETE FROM data_import_global.queue_items WHERE id = $1"),
              any(Tuple.class)
            );
          verifyNoMoreInteractions(postgresClient);
        })
      );
  }
}
