package org.folio.service.s3storage;

import io.vertx.core.Future;
import org.folio.rest.jaxrs.model.UserInfo;

public interface MinioStorageService {

  /*
   *
   */
  Future<UserInfo> getFileUploadUrl(String upLoadFileName, String tenantId);


}
