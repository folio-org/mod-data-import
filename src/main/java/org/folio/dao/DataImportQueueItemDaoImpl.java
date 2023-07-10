package org.folio.dao;

import static java.lang.String.format;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.ws.rs.NotFoundException;
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
    "INSERT INTO %s.%s (id, jobExecutionId, uploadDefinitionId, tenant, size, originalSize, filePath, timestamp, partNumber, processing) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";
  private static final String UPDATE_BY_ID_SQL =
    "UPDATE %s.%s SET jobExecutionId = $2,uploadDefinitionId = $3, tenant = $4,size = $5,originalSize = $6,filePath = $7,timestamp = $8, partNumber = $9, processing = $10 WHERE id = $1";
  private static final String DELETE_BY_ID_SQL =
    "DELETE FROM %s.%s WHERE id = $1";

  @Autowired
  private PostgresClientFactory pgClientFactory;

  public DataImportQueueItemDaoImpl() {
    super();
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
    return promise.future().map(this::mapResultSetToQueueItemList);
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
    return promise.future().map(this::mapResultSetToQueueItemList);
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
    return promise.future().map(this::mapResultSetToQueueItemList);
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
      .map(resultSet ->
        resultSet.rowCount() == 0
          ? Optional.empty()
          : Optional.of(mapRowJsonToQueueItem(resultSet.iterator().next()))
      );
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
          dataImportQueueItem.getTenant(),
          dataImportQueueItem.getUploadDefinitionId(),
          dataImportQueueItem.getSize(),
          dataImportQueueItem.getOriginalSize(),
          dataImportQueueItem.getFilePath(),
          dataImportQueueItem.getTimestamp(),
          dataImportQueueItem.getPartNumber(),
          dataImportQueueItem.getProcessing()
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
            dataImportQueueItem.getSize(),
            dataImportQueueItem.getOriginalSize(),
            dataImportQueueItem.getFilePath(),
            dataImportQueueItem.getTimestamp(),
            dataImportQueueItem.getPartNumber(),
            dataImportQueueItem.getProcessing()
          ),
          promise
        );
    } catch (Exception e) {
      LOGGER.error("Error updating queue Item %s", dataImportQueueItem.getId());
      promise.fail(e);
    }
    return promise
      .future()
      .compose(updateResult ->
        updateResult.rowCount() == 1
          ? Future.succeededFuture(dataImportQueueItem)
          : Future.failedFuture(
            new NotFoundException(
              format(
                "DataImportQueueItem with id %s was not updated",
                dataImportQueueItem.getId()
              )
            )
          )
      );
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
      .flatMap(result -> {
        if (result.rowCount() == 1) {
          return Future.succeededFuture();
        }
        String message = format(
          "Error deleting Queue Item with event id '%s'",
          id
        );
        NotFoundException notFoundException = new NotFoundException(message);
        LOGGER.error(message, notFoundException);
        return Future.failedFuture(notFoundException);
      });
  }

  private DataImportQueueItem mapRowJsonToQueueItem(Row rowAsJson) {
    DataImportQueueItem queueItem = new DataImportQueueItem();
    queueItem.setId(rowAsJson.getString("id"));
    queueItem.setJobExecutionId(rowAsJson.getString("jobExeutionId"));
    queueItem.setUploadDefinitionId(rowAsJson.getString("uploadDefinitionId"));
    queueItem.setTenant(rowAsJson.getString("tenant"));
    queueItem.setFilePath(rowAsJson.getString("filePath"));
    queueItem.setSize(rowAsJson.getInteger("size"));
    queueItem.setOriginalSize(rowAsJson.getInteger("originalSize"));
    queueItem.setTimestamp(rowAsJson.getString("timeStamp"));
    queueItem.setPartNumber(rowAsJson.getInteger("partNumber"));
    queueItem.setProcessing(rowAsJson.getBoolean("processing"));
    return queueItem;
  }

  private DataImportQueueItemCollection mapResultSetToQueueItemList(
    RowSet<Row> resultSet
  ) {
    DataImportQueueItemCollection result = new DataImportQueueItemCollection();
    result.setDataImportQueueItems(
      Stream
        .generate(resultSet.iterator()::next)
        .limit(resultSet.size())
        .map(this::mapRowJsonToQueueItem)
        .collect(Collectors.toList())
    );
    return result;
  }
}
