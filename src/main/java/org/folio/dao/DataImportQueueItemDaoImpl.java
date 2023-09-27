package org.folio.dao;

import static java.lang.String.format;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.pgclient.PgConnection;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Date;
import java.util.Optional;
import java.util.TimeZone;
import java.util.UUID;
import java.util.function.BiFunction;
import java.util.stream.Stream;
import javax.ws.rs.NotFoundException;
import org.apache.commons.lang3.time.TimeZones;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.folio.rest.jaxrs.model.DataImportQueueItemCollection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

@Repository
public class DataImportQueueItemDaoImpl implements DataImportQueueItemDao {

  private static final Logger LOGGER = LogManager.getLogger();

  private static final String MODULE_GLOBAL_SCHEMA = "data_import_global";
  private static final String QUEUE_ITEM_TABLE = "queue_items";
  private static final String GET_ALL_SQL = "SELECT * FROM %s.%s";
  private static final String GET_ALL_BY_PROCESSING_SQL =
    "SELECT * FROM %s.%s WHERE processing = $1";
  private static final String GET_BY_ID_SQL =
    "SELECT * FROM %s.%s WHERE id = $1";
  private static final String INSERT_SQL =
    "INSERT INTO %s.%s (id, job_execution_id, upload_definition_id, tenant, original_size, file_path, timestamp, part_number, processing, okapi_url, data_type) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)";
  private static final String UPDATE_BY_ID_SQL =
    "UPDATE %s.%s SET job_execution_id = $2, upload_definition_id = $3, tenant = $4, original_size = $5, file_path = $6, timestamp = $7, part_number = $8, processing = $9, okapi_url = $10, data_type = $11 WHERE id = $1";
  private static final String DELETE_BY_ID_SQL =
    "DELETE FROM %s.%s WHERE id = $1";
  private static final String DELETE_BY_JOB_ID_SQL =
    "DELETE FROM %s.%s WHERE job_execution_id = $1";
  private static final String LOCK_ACCESS_EXCLUSIVE_SQL =
    "LOCK TABLE %s.%s IN ACCESS EXCLUSIVE MODE";

  private static final String DATE_FORMAT_PATTERN =
    "yyyy-MM-dd'T'HH:mm:ss.SSSX";

  private PostgresClientFactory pgClientFactory;
  private SimpleDateFormat dateFormatter;
  private TimeZone timeZone;

  @Autowired
  public DataImportQueueItemDaoImpl(PostgresClientFactory pgClientFactory) {
    this.pgClientFactory = pgClientFactory;

    this.timeZone = TimeZone.getTimeZone(TimeZones.GMT_ID);
    this.dateFormatter = new SimpleDateFormat(DATE_FORMAT_PATTERN);
    this.dateFormatter.setTimeZone(this.timeZone);
  }

  @Override
  public Future<DataImportQueueItemCollection> getAllQueueItems() {
    Promise<RowSet<Row>> promise = Promise.promise();
    try {
      String preparedQuery = format(
        GET_ALL_SQL,
        MODULE_GLOBAL_SCHEMA,
        QUEUE_ITEM_TABLE
      );
      pgClientFactory.getInstance().select(preparedQuery, promise);
    } catch (Exception e) {
      LOGGER.warn(
        "getDataImportQueueItem:: Error while searching for all DataImportQueueItems",
        e
      );
      promise.fail(e);
    }
    return promise
      .future()
      .map(DataImportQueueItemDaoImpl::mapResultSetToQueueItemList);
  }

  @Override
  public Future<DataImportQueueItemCollection> getAllWaitingQueueItems() {
    Promise<RowSet<Row>> promise = Promise.promise();
    try {
      String preparedQuery = format(
        GET_ALL_BY_PROCESSING_SQL,
        MODULE_GLOBAL_SCHEMA,
        QUEUE_ITEM_TABLE
      );
      pgClientFactory
        .getInstance()
        .select(preparedQuery, Tuple.of(false), promise);
    } catch (Exception e) {
      LOGGER.warn(
        "getDataImportQueueItem:: Error while searching for waiting DataImportQueueItems",
        e
      );
      promise.fail(e);
    }
    return promise
      .future()
      .map(DataImportQueueItemDaoImpl::mapResultSetToQueueItemList);
  }

  @Override
  public Future<DataImportQueueItemCollection> getAllInProgressQueueItems() {
    Promise<RowSet<Row>> promise = Promise.promise();
    try {
      String preparedQuery = format(
        GET_ALL_BY_PROCESSING_SQL,
        MODULE_GLOBAL_SCHEMA,
        QUEUE_ITEM_TABLE
      );
      pgClientFactory
        .getInstance()
        .select(preparedQuery, Tuple.of(true), promise);
    } catch (Exception e) {
      LOGGER.warn(
        "getDataImportQueueItem:: Error while searching for in progress DataImportQueueItems",
        e
      );
      promise.fail(e);
    }
    return promise
      .future()
      .map(DataImportQueueItemDaoImpl::mapResultSetToQueueItemList);
  }

  @Override
  public Future<Optional<DataImportQueueItem>> getAllQueueItemsAndProcessAtomic(
    BiFunction<DataImportQueueItemCollection, DataImportQueueItemCollection, Optional<DataImportQueueItem>> processor
  ) {
    return pgClientFactory
      .getInstance()
      .withTransaction((PgConnection conn) -> {
        // lock the table to ensure no other workers can read or update
        conn.query(
          format(
            LOCK_ACCESS_EXCLUSIVE_SQL,
            MODULE_GLOBAL_SCHEMA,
            QUEUE_ITEM_TABLE
          )
        );

        return CompositeFuture
          .all(getAllInProgressQueueItems(), getAllWaitingQueueItems())
          .compose((CompositeFuture compositeFuture) -> {
            DataImportQueueItemCollection inProgress = compositeFuture.resultAt(
              0
            );
            DataImportQueueItemCollection waiting = compositeFuture.resultAt(1);

            Optional<DataImportQueueItem> result = processor.apply(
              inProgress,
              waiting
            );

            return Future.succeededFuture(result);
          })
          .compose((Optional<DataImportQueueItem> result) -> {
            if (result.isPresent()) {
              return updateDataImportQueueItem(
                result.get().withProcessing(true)
              )
                .map(Optional::of);
            }
            return Future.succeededFuture(result);
          });
      });
  }

  @Override
  public Future<Optional<DataImportQueueItem>> getQueueItemById(String id) {
    Promise<RowSet<Row>> promise = Promise.promise();
    try {
      String preparedQuery = format(
        GET_BY_ID_SQL,
        MODULE_GLOBAL_SCHEMA,
        QUEUE_ITEM_TABLE
      );
      pgClientFactory
        .getInstance()
        .select(preparedQuery, Tuple.of(id), promise);
    } catch (Exception e) {
      LOGGER.warn(
        "getDataImportQueueItem:: Error while searching for DataImportQueueItem by ID",
        e
      );
      promise.fail(e);
    }
    return promise
      .future()
      .map((RowSet<Row> resultSet) -> {
        if (resultSet.rowCount() == 0) {
          return Optional.empty();
        } else {
          return Optional.of(
            mapRowJsonToQueueItem(resultSet.iterator().next())
          );
        }
      });
  }

  @Override
  public Future<String> addQueueItem(DataImportQueueItem dataImportQueueItem) {
    Promise<RowSet<Row>> promise = Promise.promise();
    String preparedQuery = format(
      INSERT_SQL,
      MODULE_GLOBAL_SCHEMA,
      QUEUE_ITEM_TABLE
    );
    pgClientFactory
      .getInstance()
      .execute(
        preparedQuery,
        Tuple.of(
          dataImportQueueItem.getId(),
          dataImportQueueItem.getJobExecutionId(),
          dataImportQueueItem.getUploadDefinitionId(),
          dataImportQueueItem.getTenant(),
          dataImportQueueItem.getOriginalSize(),
          dataImportQueueItem.getFilePath(),
          LocalDateTime.ofInstant(
            dataImportQueueItem.getTimestamp().toInstant(),
            timeZone.toZoneId()
          ),
          dataImportQueueItem.getPartNumber(),
          dataImportQueueItem.getProcessing(),
          dataImportQueueItem.getOkapiUrl(),
          dataImportQueueItem.getDataType()
        ),
        promise
      );
    return promise.future().map(updateResult -> dataImportQueueItem.getId());
  }

  @Override
  public Future<DataImportQueueItem> updateDataImportQueueItem(
    DataImportQueueItem dataImportQueueItem
  ) {
    Promise<RowSet<Row>> promise = Promise.promise();
    try {
      String query = format(
        UPDATE_BY_ID_SQL,
        MODULE_GLOBAL_SCHEMA,
        QUEUE_ITEM_TABLE
      );
      pgClientFactory
        .getInstance()
        .execute(
          query,
          Tuple.of(
            dataImportQueueItem.getId(),
            dataImportQueueItem.getJobExecutionId(),
            dataImportQueueItem.getUploadDefinitionId(),
            dataImportQueueItem.getTenant(),
            dataImportQueueItem.getOriginalSize(),
            dataImportQueueItem.getFilePath(),
            LocalDateTime.ofInstant(
              dataImportQueueItem.getTimestamp().toInstant(),
              timeZone.toZoneId()
            ),
            dataImportQueueItem.getPartNumber(),
            dataImportQueueItem.getProcessing(),
            dataImportQueueItem.getOkapiUrl(),
            dataImportQueueItem.getDataType()
          ),
          promise
        );
    } catch (Exception e) {
      LOGGER.error("Error updating queue Item %s", dataImportQueueItem.getId());
      promise.fail(e);
    }
    return promise
      .future()
      .compose((RowSet<Row> updateResult) -> {
        if (updateResult.rowCount() == 1) {
          return Future.succeededFuture(dataImportQueueItem);
        } else {
          return Future.failedFuture(
            new NotFoundException(
              format(
                "DataImportQueueItem with id %s was not updated",
                dataImportQueueItem.getId()
              )
            )
          );
        }
      });
  }

  @Override
  public Future<Void> deleteDataImportQueueItemByJobExecutionId(String id) {
    String query = format(
      DELETE_BY_JOB_ID_SQL,
      MODULE_GLOBAL_SCHEMA,
      QUEUE_ITEM_TABLE
    );
    return pgClientFactory
      .getInstance()
      .execute(query, Tuple.of(id))
      .flatMap((RowSet<Row> result) -> {
        if (result.rowCount() == 1) {
          return Future.succeededFuture();
        }
        String message = format(
          "Error deleting queue item with job execution id '%s'",
          id
        );
        NotFoundException notFoundException = new NotFoundException(message);
        LOGGER.error(message, notFoundException);
        return Future.failedFuture(notFoundException);
      });
  }

  @Override
  public Future<Void> deleteDataImportQueueItem(String id) {
    String query = format(
      DELETE_BY_ID_SQL,
      MODULE_GLOBAL_SCHEMA,
      QUEUE_ITEM_TABLE
    );
    return pgClientFactory
      .getInstance()
      .execute(query, Tuple.of(id))
      .flatMap((RowSet<Row> result) -> {
        if (result.rowCount() == 1) {
          return Future.succeededFuture();
        }
        String message = format("Error deleting queue item with id '%s'", id);
        NotFoundException notFoundException = new NotFoundException(message);
        LOGGER.error(message, notFoundException);
        return Future.failedFuture(notFoundException);
      });
  }

  private static DataImportQueueItem mapRowJsonToQueueItem(Row rowAsJson) {
    DataImportQueueItem queueItem = new DataImportQueueItem();
    queueItem.setId(rowAsJson.get(UUID.class, "id").toString());
    queueItem.setJobExecutionId(
      rowAsJson.get(UUID.class, "job_execution_id").toString()
    );
    queueItem.setUploadDefinitionId(
      rowAsJson.get(UUID.class, "upload_definition_id").toString()
    );
    queueItem.setTenant(rowAsJson.getString("tenant"));
    queueItem.setFilePath(rowAsJson.getString("file_path"));
    queueItem.setOriginalSize(rowAsJson.getInteger("original_size"));
    queueItem.setTimestamp(
      Date.from(LocalDateTime.now().toInstant(ZoneOffset.UTC))
    );
    queueItem.setPartNumber(rowAsJson.getInteger("part_number"));
    queueItem.setProcessing(rowAsJson.getBoolean("processing"));
    queueItem.setOkapiUrl(rowAsJson.getString("okapi_url"));
    queueItem.setDataType(rowAsJson.getString("data_type"));
    return queueItem;
  }

  private static DataImportQueueItemCollection mapResultSetToQueueItemList(
    RowSet<Row> resultSet
  ) {
    DataImportQueueItemCollection result = new DataImportQueueItemCollection();
    result.setDataImportQueueItems(
      Stream
        .generate(resultSet.iterator()::next)
        .limit(resultSet.size())
        .map(DataImportQueueItemDaoImpl::mapRowJsonToQueueItem)
        .toList()
    );
    return result;
  }
}
