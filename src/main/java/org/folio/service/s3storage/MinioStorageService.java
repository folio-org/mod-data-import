package org.folio.service.s3storage;

import io.vertx.core.Future;

public interface MinioStorageService {

  /*
   *
   */
  Future<String> getFileUploadUrl( String upLoadFileName, String tenantId);


}
