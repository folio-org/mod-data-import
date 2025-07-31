package org.folio.dao;

import io.vertx.core.Future;
import java.util.Optional;
import java.util.function.BiFunction;

import io.vertx.pgclient.PgConnection;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.folio.rest.jaxrs.model.DataImportQueueItemCollection;

public interface DataImportQueueItemDao {
  /**
   * Get all {@link DataImportQueueItem} in database
   *
   * @return future with {@link DataImportQueueItemCollection}
   */
  Future<DataImportQueueItemCollection> getAllQueueItems();

  /**
   * Get all {@link DataImportQueueItem} in database where processing=false
   *
   * @return future with {@link DataImportQueueItemCollection}
   */
  Future<DataImportQueueItemCollection> getAllWaitingQueueItems(PgConnection connection);

  /**
   * Get all {@link DataImportQueueItem} in database where processing=true
   *
   * @return future with {@link DataImportQueueItemCollection}
   */
  Future<DataImportQueueItemCollection> getAllInProgressQueueItems(PgConnection connection);

  /**
   * Get all in progress and waiting queue items, send them through the
   * {@code processor}, and mark the processor's output as
   * {@code processing=true}.
   *
   * This is done as <strong>one atomic operation</strong>, ensuring that no
   * other worker can process the same queue item.
   *
   * @param processor a function which takes collections of in progress
   *                  and waiting queue items, determines which should be
   *                  marked as {@code processing=true} (if any), and returns
   *                  that item
   * @return the item returned by the {@code processor}
   */
  Future<Optional<DataImportQueueItem>> getAllQueueItemsAndProcessAtomic(
    BiFunction<DataImportQueueItemCollection, DataImportQueueItemCollection, Optional<DataImportQueueItem>> processor
  );

  /**
   * Searches for {@link DataImportQueueItem} by id
   *
   * @param id DataImportQueueItem id
   * @return future which will resolve with the requested item
   */
  Future<DataImportQueueItem> getQueueItemById(String id);

  /**
   * Saves {@link DataImportQueueItem} to database
   *
   * @param DataImportQueueItem DataImportQueueItem to save
   * @return future with added row's ID
   */
  Future<String> addQueueItem(DataImportQueueItem dataImportQueueItem);

  /**
   * Updates {@link DataImportQueueItem} in database
   *
   * @param fileExtension FileExtension to update
   * @param conn
   * @return future with {@link DataImportQueueItem}
   */
  Future<DataImportQueueItem> updateQueueItem(
    PgConnection conn, DataImportQueueItem dataImportQueueItem
  );

  /**
   * Deletes {@link DataImportQueueItem} from database
   *
   * @param id DataImportQueueItem id
   * @return future that resolves if a record was deleted, failing otherwise
   */
  Future<Void> deleteQueueItemById(String id);

  /**
   * Deletes all {@link DataImportQueueItem}s from database with the provided job execution ID.
   *
   * @param id DataImportQueueItem id
   * @return future that resolves with the number of deleted rows, failing if none were deleted
   */
  Future<Integer> deleteQueueItemsByJobExecutionId(String jobExecution);
}
