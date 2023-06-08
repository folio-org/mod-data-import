package org.folio.service.s3storage;

import io.vertx.core.Future;
import org.folio.rest.jaxrs.model.FileUploadInfo;

public interface MinioStorageService {

  /*
   *
   */
  Future<FileUploadInfo> getFileUploadUrl(String upLoadFileName, String tenantId);


}
