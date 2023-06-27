package org.folio.dao;

import static java.lang.String.format;

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

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.Tuple;

@Repository
public class DataImportQueueItemDaoImpl implements DataImportQueueItemDao {

  private static final Logger LOGGER = LogManager.getLogger();
  
  private static String MODULE_GLOBAL_SCHEMA = "data_import_global";
  private static final String QUEUE_ITEM_TABLE = "queue_items";
  private static final String GET_ALL_SQL = "SELECT * FROM %s.%s";
  private static final String GET_BY_ID_SQL = "SELECT * FROM %s.%s WHERE id = $1";
  private static final String INSERT_SQL = "INSERT INTO %s.%s (id, jobExecutionId,uploadDefinitionId,size,originalSize,filePath,timestamp) VALUES ($1, $2, $3, $4, $5, $6, $7)";
  private static final String UPDATE_BY_ID_SQL = "UPDATE %s.%s SET jobExecutionId = $2,uploadDefinitionId = $3,size = $4,originalSize = $5,filePath = $6,timestamp = $7 WHERE id = $1";
  private static final String DELETE_BY_ID_SQL = "DELETE FROM %s.%s WHERE id = $1";
  
  @Autowired
  private PostgresClientFactory pgClientFactory;
  
  public DataImportQueueItemDaoImpl() {
    super();
  }
 
  @Override
  public Future<DataImportQueueItemCollection> getQueueItem(String query, int offset, int limit) {
    Promise<RowSet<Row>> promise = Promise.promise();
    try {
      String preparedQuery = format(GET_ALL_SQL, MODULE_GLOBAL_SCHEMA, QUEUE_ITEM_TABLE);
      pgClientFactory.getInstance().select(preparedQuery, promise);
    } catch (Exception e) {
      LOGGER.warn("getDataImportQueueItem:: Error while searching for DataImportQueueItem", e);
      promise.fail(e);
    }
    return promise.future().map(this::mapResultSetToQueueItemList);
  }

  @Override
  public Future<Optional<DataImportQueueItem>> getQueueItemById(String id) {
   
    Promise<RowSet<Row>> promise = Promise.promise();
    try {
      String preparedQuery = format(GET_BY_ID_SQL, MODULE_GLOBAL_SCHEMA, QUEUE_ITEM_TABLE);
      pgClientFactory.getInstance().select(preparedQuery,Tuple.of(id), promise);
      
    } catch (Exception e) {
      LOGGER.warn("getDataImportQueueItem:: Error while searching for DataImportQueueItem", e);
      promise.fail(e);
    }
    return promise.future().map(resultSet -> resultSet.rowCount() == 0 ? Optional.empty()
        : Optional.of(mapRowJsonToQueueItem(resultSet.iterator().next())));
  }

  @Override
  public Future<String> addQueueItem(DataImportQueueItem dataImportQueueItem) {
    Promise<RowSet<Row>> promise = Promise.promise();
    String preparedQuery = format(INSERT_SQL, MODULE_GLOBAL_SCHEMA, QUEUE_ITEM_TABLE );
    pgClientFactory.getInstance().execute(preparedQuery,  Tuple.of(dataImportQueueItem.getId(),
        dataImportQueueItem.getJobExecutionId(),dataImportQueueItem.getUploadDefinitionId(),
        dataImportQueueItem.getSize(), dataImportQueueItem.getOriginalSize(),dataImportQueueItem.getFilePath(),
        dataImportQueueItem.getTimestamp()),promise);
    return promise.future().map(updateResult -> dataImportQueueItem.getId());
  }

  @Override
  public Future<DataImportQueueItem> updateDataImportQueueItem(DataImportQueueItem dataImportQueueItem) {
    Promise<RowSet<Row>> promise = Promise.promise();
    try {
      String query = format(UPDATE_BY_ID_SQL, MODULE_GLOBAL_SCHEMA, QUEUE_ITEM_TABLE);
      pgClientFactory.getInstance().execute(query, Tuple.of(dataImportQueueItem.getId(),
          dataImportQueueItem.getJobExecutionId(),dataImportQueueItem.getUploadDefinitionId(),
          dataImportQueueItem.getSize(), dataImportQueueItem.getOriginalSize(),dataImportQueueItem.getFilePath(),
          dataImportQueueItem.getTimestamp()),
         promise);
    } catch (Exception e) {
      LOGGER.error("Error updating queue Item ", dataImportQueueItem, e);
      promise.fail(e);
    }
    return promise.future().compose(updateResult -> updateResult.rowCount() == 1
        ? Future.succeededFuture(dataImportQueueItem)
        : Future.failedFuture(new NotFoundException(format("DataImportQueueItem with id %s was not updated", dataImportQueueItem.getId()))));
  }

  @Override
  public Future<Void> deleteDataImportQueueItem(String id) {
   
    String query = format(DELETE_BY_ID_SQL, MODULE_GLOBAL_SCHEMA, QUEUE_ITEM_TABLE);
    return pgClientFactory.getInstance().execute(query, Tuple.of(id)).flatMap(result -> {
      if (result.rowCount() == 1) {
        return Future.succeededFuture();
      }
        String message = format("Error deleting Queue Item with event id '%s'", id);
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
    queueItem.setFilePath(rowAsJson.getString("filePath"));
    queueItem.setSize(rowAsJson.getInteger("size"));
    queueItem.setOriginalSize(rowAsJson.getInteger("originalSize"));
    queueItem.setTimestamp(rowAsJson.getString("timeStamp"));
    return queueItem;
  }

  private DataImportQueueItemCollection mapResultSetToQueueItemList(RowSet<Row> resultSet) {
    DataImportQueueItemCollection result = new DataImportQueueItemCollection();
        result.setDataImportQueueItems(Stream.generate(resultSet.iterator()::next)
      .limit(resultSet.size())
      .map(this::mapRowJsonToQueueItem)
      .collect(Collectors.toList()));
        return result;
  }
}
