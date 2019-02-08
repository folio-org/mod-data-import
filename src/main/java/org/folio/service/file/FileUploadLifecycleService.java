package org.folio.service.file;

import io.vertx.core.Future;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.FileDefinition;
import org.folio.rest.jaxrs.model.UploadDefinition;

/**
 * File service
 */

public interface FileUploadLifecycleService {

  Future<UploadDefinition> beforeFileSave(String fileId, String uploadDefinitionId, OkapiConnectionParams params);

  Future<UploadDefinition> afterFileSave(FileDefinition fileDefinition, OkapiConnectionParams params);

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
