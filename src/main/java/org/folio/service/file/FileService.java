package org.folio.service.file;

import io.vertx.core.Future;
import org.folio.dataImport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.UploadDefinition;

import java.io.InputStream;

/**
 * File service
 */

public interface FileService {

  /**
   * Saves File with uploaded data
   *
   * @param fileId             id of File to save
   * @param uploadDefinitionId id of Upload Definition
   * @param data               stream with uploaded data
   * @return future with {@link org.folio.rest.jaxrs.model.UploadDefinition} id
   */
  Future<UploadDefinition> uploadFile(String fileId, String uploadDefinitionId, InputStream data, OkapiConnectionParams params);

  /**
   * Deletes File by id and Upload Definition Id
   *
   * @param id                 File id
   * @param uploadDefinitionId Upload Definition id
   * @return future with true is succeeded
   */
  Future<Boolean> deleteFile(String id, String uploadDefinitionId);

}
