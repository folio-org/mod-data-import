package org.folio.service.s3storage;

import io.vertx.core.Future;
import org.folio.rest.jaxrs.model.FileUploadInfo;

public interface MinioStorageService {
  /*
   * Gets upload url and key for a file upload
   *
   * @param uploadFileName - name of file to be uploaded
   * @param tenantId - tenant associated with this upload
   *
   * @return FileUploadInfo
   *  url - presigned Url to S3 storage
   *  key - key for access to file on S3 storage
   */
  Future<FileUploadInfo> getFileUploadUrl(
    String uploadFileName,
    String tenantId
  );
}
