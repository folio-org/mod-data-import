package org.folio.service.s3storage;

import io.vertx.core.Future;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import javax.validation.constraints.NotNull;
import org.folio.rest.jaxrs.model.FileDownloadInfo;
import org.folio.rest.jaxrs.model.FileUploadInfo;

public interface MinioStorageService {
  /**
   * Gets upload url and key for a file upload
   *
   * @param uploadFileName - name of file to be uploaded
   * @param tenantId - tenant associated with this upload
   * @param uploadId - id of upload, if this upload will continue on a previously
   * started multipart upload
   * @param partNumber - the part number, starting at 1. If uploadId=null, this
   * must be 1.
   * @return FileUploadInfo
   * <ul>
   *   <li>url: presigned Url to S3 storage</li>
   *   <li>key: key for access to file on S3 storage</li>
   *   <li>uploadId: multipart upload ID</li>
   * </ul>
   */
  Future<FileUploadInfo> getFileUploadFirstPartUrl(
    String uploadFileName,
    String tenantId
  );

  /**
   * Gets upload url and key for a file upload
   *
   * @param key - the key to access the file on S3 storage, as generated with
   *   {@link #getFileFirstPartUploadUrl}
   * @param uploadId - id of upload from previous calls to {@link #getFileFirstPartUploadUrl}
   * @param partNumber - the part number, starting at 2 (part number 1 will be
   *   provided by {@link #getFileFirstPartUploadUrl})
   * @return FileUploadInfo
   * <ul>
   *   <li>url: presigned URL to S3 storage</li>
   *   <li>key: key for access to file on S3 storage</li>
   *   <li>uploadId: multipart upload ID</li>
   * </ul>
   */
  Future<FileUploadInfo> getFileUploadPartUrl(
    String key,
    @NotNull String uploadId,
    int partNumber
  );

  /**
   * Gets a presigned download url
   *
   * @param key - the key to access the file on S3 storage, as stored in the job execution in mod-srm
   * @return FileDownloadInfo
   * <ul>
   *   <li>url: presigned URL to S3 storage</li>
   * </ul>
   */
  Future<FileDownloadInfo> getFileDownloadUrl(String key);

  Future<Boolean> completeMultipartFileUpload(
    String key,
    String uploadId,
    List<String> etags
  );

  /**
   * Opens a file on S3, returns an input stream to read from the remote file
   * The calling method is responsible for closing the stream
   *
   * @param key - the key to access the file on S3 storage
   * @return InputStream - a new input stream with file content
   */
  Future<InputStream> readFile(String key);

  /**
   * Writes bytes to a file on S3-compatible storage
   *
   * @param path the path to the file on S3-compatible storage
   * @param is   the byte array with the bytes to write
   * @return the path to the file
   */
  Future<String> write(String path, InputStream is) throws IOException;

  /**
   * Delete a file from S3 storage
   * @param key the key to delete
   * @return a future that will be completed upon file deletion
   */
  Future<Void> remove(String key);
}
