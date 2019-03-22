package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.ext.sql.UpdateResult;
import org.folio.rest.jaxrs.model.FileExtension;
import org.folio.rest.jaxrs.model.FileExtensionCollection;

import java.util.Optional;

/**
 * Data access object for {@link FileExtension}
 */
public interface FileExtensionDao {

  /**
   * Searches for {@link FileExtension} in database
   *
   * @param query  query from URL
   * @param offset starting index in a list of results
   * @param limit  limit of records for pagination
   * @param tenantId tenant id tenant id
   * @return future with {@link FileExtensionCollection}
   */
  Future<FileExtensionCollection> getFileExtensions(String query, int offset, int limit, String tenantId);

  /**
   * Searches for {@link FileExtension} by id
   *
   * @param id FileExtension id
   * @param tenantId tenant id tenant id
   * @return future with optional {@link FileExtension}
   */
  Future<Optional<FileExtension>> getFileExtensionById(String id, String tenantId);

  /**
   * Searches for {@link FileExtension} by its extension
   *
   * @param extension File Extension
   * @param tenantId tenant id tenant id
   * @return future with optional {@link FileExtension}
   */
  Future<Optional<FileExtension>> getFileExtensionByExtenstion(String extension, String tenantId);

  /**
   * Searches for all {@link FileExtension} in database from selected table
   *
   * @return future with {@link FileExtensionCollection}
   */
  Future<FileExtensionCollection> getAllFileExtensionsFromTable(String tableName, String tenantId);

  /**
   * Saves {@link FileExtension} to database
   *
   * @param fileExtension FileExtension to save
   * @param tenantId tenant id tenant id
   * @return future
   */
  Future<String> addFileExtension(FileExtension fileExtension, String tenantId);

  /**
   * Updates {@link FileExtension} in database
   *
   * @param fileExtension FileExtension to update
   * @param tenantId tenant id
   * @return future with {@link FileExtension}
   */
  Future<FileExtension> updateFileExtension(FileExtension fileExtension, String tenantId);

  /**
   * Deletes {@link FileExtension} from database
   *
   * @param id FileExtension id
   * @param tenantId tenant id
   * @return future with true if succeeded
   */
  Future<Boolean> deleteFileExtension(String id, String tenantId);

  /**
   * Restore default values for {@link FileExtension}
   *
   * @return - future with restored file extensions
   * @param tenantId tenant id
   */
  Future<FileExtensionCollection> restoreFileExtensions(String tenantId);

  /**
   * Copy values from default_file_extensions into the file_extensions table
   *
   * @return - Update Result of coping execution
   * @param tenantId tenant id
   */
  Future<UpdateResult> copyExtensionsFromDefault(String tenantId);
}
