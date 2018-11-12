package org.folio.service;

import io.vertx.core.Future;
import org.folio.rest.jaxrs.model.File;

import java.io.InputStream;
import java.util.List;
import java.util.Optional;

/**
 * File service
 */

public interface FileService {

  /**
   * Searches for Files
   *
   * @param query  CQL query
   * @param offset offset
   * @param limit  limit
   * @return future with list of Files
   */
  Future<List<File>> getFiles(String query, int offset, int limit);

  /**
   * Searches for File by id
   *
   * @param id File id
   * @return future with optional File
   */
  Future<Optional<File>> getFileById(String id);

  /**
   * Searches for File by upload definition id
   *
   * @param id File id
   * @return future with optional File
   */
  Future<List<File>> getFileByUploadDefinitionId(String id);

  /**
   * Saves File with generated id
   *
   * @param file File to save
   * @return future with generated id
   */
  Future<String> addFile(File file);


  /**
   * Saves File with uploaded data
   *
   * @param fileId id of File to save
   * @return future with {@link org.folio.rest.jaxrs.model.UploadDefinition} id
   */
  Future<String> uploadFile(String fileId, InputStream data);

  /**
   * Updates File with given id
   *
   * @param file File to update
   * @return future with true is succeeded
   */
  Future<Boolean> updateFile(File file);

  /**
   * Deletes File by id and Upload Definition Id
   *
   * @param id                 File id
   * @param uploadDefinitionId Upload Definition id
   * @return future with true is succeeded
   */
  Future<Boolean> deleteFile(String id, String uploadDefinitionId);

  /**
   * Deletes Files by uploadDefinitionId
   *
   * @param uploadDefinitionId Upload Definition id
   * @return future with true is succeeded
   */
  Future<Boolean> deleteFilesByUploadDefinitionId(String uploadDefinitionId);

}
