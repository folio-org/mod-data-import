package org.folio.service.file;

import io.vertx.core.Future;
import org.folio.rest.jaxrs.model.UploadDefinition;

import java.io.InputStream;
import java.util.Map;

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
  Future<UploadDefinition> uploadFile(String fileId, String uploadDefinitionId, InputStream data, Map<String, String> okapiHeaders);

  /**
   * Deletes File by id and Upload Definition Id
   *
   * @param id                 File id
   * @param uploadDefinitionId Upload Definition id
   * @return future with true is succeeded
   */
  Future<Boolean> deleteFile(String id, String uploadDefinitionId);

}
