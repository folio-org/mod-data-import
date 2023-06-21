package org.folio.dao;

import static org.folio.dataimport.util.DaoUtil.getCQLWrapper;

import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.folio.rest.jaxrs.model.DataImportQueueItemCollection;

import org.folio.rest.persist.cql.CQLWrapper;
import org.folio.rest.persist.interfaces.Results;
import org.springframework.beans.factory.annotation.Autowired;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

public class DataImportFileQueueItemDaoImpl implements DataImportFileQueueItemDao {

  private static final Logger LOGGER = LogManager.getLogger();
  
  private static String MODULE_GLOBAL_SCHEMA = "data_import_global";
  private static final String QUEUE_ITEM_TABLE = "default_file_extensions";

  
  @Autowired
  private PostgresClientFactory pgClientFactory;
  
  public DataImportFileQueueItemDaoImpl(Vertx vertx) {
    super();
    pgClientFactory = new PostgresClientFactory(vertx);
  }
  
  @Override
  public Future<DataImportQueueItemCollection> getQueueItem(String query, int offset, int limit) {
    Promise<Results<DataImportQueueItem>> promise = Promise.promise();
    try {
      String[] fieldList = {"*"};
      CQLWrapper cql = getCQLWrapper(QUEUE_ITEM_TABLE, query, limit, offset);
      pgClientFactory.createInstance(MODULE_GLOBAL_SCHEMA).get(QUEUE_ITEM_TABLE, DataImportQueueItem.class, fieldList, cql, true, false, promise);
    } catch (Exception e) {
      LOGGER.warn("getFileExtensions:: Error while searching for FileExtensions", e);
      promise.fail(e);
    }
    return promise.future().map(results -> new DataImportQueueItemCollection()
      .withDataImportQueueItems(results.getResults())
      .withTotalRecords(results.getResultInfo().getTotalRecords()));
  }

  @Override
  public Future<Optional<DataImportQueueItem>> getQueueItemById(String id) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Future<String> addQueueItem(DataImportQueueItem dataImportQueueItem) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Future<DataImportQueueItem> updateDataImportQueueItem(DataImportQueueItem dataImportQueueItem) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Future<Boolean> deleteDataImportQueueItem(String id) {
    // TODO Auto-generated method stub
    return null;
  }

}
