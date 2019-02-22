package org.folio.service.file;

import io.vertx.core.Future;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.FileDefinition;
import org.folio.rest.jaxrs.model.UploadDefinition;

/**
 * File service
 */
public interface FileUploadLifecycleService {

  /**
   * Contains business logic that runs once before saving the file when uploading
   *
   * @param fileId             - UUID for uploading {@link FileDefinition}
   * @param uploadDefinitionId - UUID for {@link UploadDefinition}
   * @param params             - {@link OkapiConnectionParams} object with connection params
   * @return - Future with completed {@link UploadDefinition} ready to start saving files data
   */
  Future<UploadDefinition> beforeFileSave(String fileId, String uploadDefinitionId, OkapiConnectionParams params);

  /**
   * Contains business logic that runs once after successfully saving the file to the  storage
   *
   * @param fileDefinition - UUID for uploading {@link FileDefinition}
   * @param params         - {@link OkapiConnectionParams} object with connection params
   * @return - Future with completed {@link UploadDefinition} for successfully saved file
   */
  Future<UploadDefinition> afterFileSave(FileDefinition fileDefinition, OkapiConnectionParams params);

  /**
   * The method is called for each piece of the file that is uploaded to the server.
   * Creates a file and writes data to it. If the file already exists, appends the data to the end of the file.
   *
   * @param fileId           - UUID for uploading {@link FileDefinition}
   * @param uploadDefinition - {@link UploadDefinition} object which describes uploading process
   * @param data             - byte array with data chunk
   * @param params           - {@link OkapiConnectionParams} object with connection params
   * @return - Future with completed {@link FileDefinition} for successfully saved file
   */
  Future<FileDefinition> saveFileChunk(String fileId, UploadDefinition uploadDefinition, byte[] data, OkapiConnectionParams params);

  /**
   * Deletes File by id and Upload Definition Id
   *
   * @param id                 File id
   * @param uploadDefinitionId Upload Definition id
   * @param params             OKAPI connection parameters
   * @return future with true is succeeded
   */
  Future<Boolean> deleteFile(String id, String uploadDefinitionId, OkapiConnectionParams params);

}
