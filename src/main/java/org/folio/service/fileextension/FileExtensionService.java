package org.folio.service.fileextension;

import io.vertx.core.Future;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.DataType;
import org.folio.rest.jaxrs.model.DataTypeCollection;
import org.folio.rest.jaxrs.model.FileExtension;
import org.folio.rest.jaxrs.model.FileExtensionCollection;

import java.util.Optional;

/**
 * FileExtension Service
 */
public interface FileExtensionService {

  /**
   * Searches for {@link FileExtension}
   *
   * @param query    query from URL
   * @param offset   starting index in a list of results
   * @param limit    limit of records for pagination
   * @param tenantId tenant id
   * @return future with {@link FileExtensionCollection}
   */
  Future<FileExtensionCollection> getFileExtensions(String query, int offset, int limit, String tenantId);

  /**
   * Searches for {@link FileExtension} by id
   *
   * @param id       FileExtension id
   * @param tenantId tenant id
   * @return future with optional {@link FileExtension}
   */
  Future<Optional<FileExtension>> getFileExtensionById(String id, String tenantId);

  /**
   * Searches for {@link FileExtension} by its extension
   *
   * @param extension File Extension
   * @param tenantId  tenant id
   * @return future with optional {@link FileExtension}
   */
  Future<Optional<FileExtension>> getFileExtensionByExtenstion(String extension, String tenantId);

  /**
   * Saves {@link FileExtension}
   *
   * @param fileExtension FileExtension to save
   * @param params        Okapi connection params
   * @return future with {@link FileExtension}
   */
  Future<FileExtension> addFileExtension(FileExtension fileExtension, OkapiConnectionParams params);

  /**
   * Updates {@link FileExtension} with given id
   *
   * @param fileExtension FileExtension to update
   * @param params        Okapi connection params
   * @return future with {@link FileExtension}
   */
  Future<FileExtension> updateFileExtension(FileExtension fileExtension, OkapiConnectionParams params);

  /**
   * Deletes {@link FileExtension} by id
   *
   * @param id       FileExtension id
   * @param tenantId tenant id
   * @return future with true if succeeded
   */
  Future<Boolean> deleteFileExtension(String id, String tenantId);

  /**
   * Restore default values for {@link FileExtension}
   *
   * @param tenantId tenant id
   * @return future with {@link FileExtensionCollection} that contains default values
   */
  Future<FileExtensionCollection> restoreFileExtensions(String tenantId);

  /**
   * Copy values from default_file_extensions into the file_extensions table
   *
   * @param tenantId tenant id
   * @return - RowSet<Row> result of copying execution
   */
  Future<RowSet<Row>> copyExtensionsFromDefault(String tenantId);

  /**
   * Returns {@link DataType}
   *
   * @return future with {@link DataTypeCollection}
   */
  Future<DataTypeCollection> getDataTypes();

  /**
   * @param fileExtension - {@link FileExtension} object
   * @param tenantId      tenant id
   * @return - is file extension exist in database
   */
  Future<Boolean> isFileExtensionExistByName(FileExtension fileExtension, String tenantId);

}
