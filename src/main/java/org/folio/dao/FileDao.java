package org.folio.dao;

import io.vertx.core.Future;
import org.folio.rest.jaxrs.model.File;

import java.util.List;
import java.util.Optional;

/**
 * Data access object for {@link File}
 */
public interface FileDao {

  /**
   * Searches for {@link File} in database
   *
   * @param query  CQL query
   * @param offset offset
   * @param limit  limit
   * @return future with list of {@link File}
   */
  Future<List<File>> getFiles(String query, int offset, int limit);

  /**
   * Searches for {@link File} by id
   *
   * @param id {@link File} id
   * @return future with optional File
   */
  Future<Optional<File>> getFileById(String id);

  /**
   * Saves {@link File} to database
   *
   * @param file {@link File} to save
   * @return future with id of saved File
   */
  Future<String> addFile(File file);

  /**
   * Updates {@link File} in database
   *
   * @param file {@link File} to update
   * @return future with true is succeeded
   */
  Future<Boolean> updateFile(File file);

  /**
   * Deletes {@link File} from database
   *
   * @param id id of {@link File} to delete
   * @return future with true is succeeded
   */
  Future<Boolean> deleteFile(String id);
}
