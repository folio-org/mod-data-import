package org.folio.dao;

import java.util.Optional;

import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.folio.rest.jaxrs.model.DataImportQueueItemCollection;

import io.vertx.core.Future;

public interface DataImportQueueItemDao {

  
  
  /**
   * Get all {@link DataImportQueueItem} in database
   *
   * @return future with {@link DataImportQueueItemCollection}
   */
  Future<DataImportQueueItemCollection> getAllQueueItems();

  /**
   * Searches for {@link DataImportQueueItem} by id
   *
   * @param id       DataImportQueueItem id
   * @return future with optional {@link DataImportQueueItem}
   */
  Future<Optional<DataImportQueueItem>>getQueueItemById(String id);



  /**
   * Saves {@link DataImportQueueItem} to database
   *
   * @param DataImportQueueItem DataImportQueueItem to save
   * @return future
   */
  Future<String> addQueueItem(DataImportQueueItem dataImportQueueItem);

  /**
   * Updates {@link DataImportQueueItem} in database
   *
   * @param fileExtension FileExtension to update
   * @return future with {@link DataImportQueueItem}
   */
  Future<DataImportQueueItem> updateDataImportQueueItem(DataImportQueueItem dataImportQueueItem);

  /**
   * Deletes {@link DataImportQueueItem} from database
   *
   * @param id       DataImportQueueItem id
   * @return future with true if succeeded
   */
  Future<Void> deleteDataImportQueueItem(String id);



}

