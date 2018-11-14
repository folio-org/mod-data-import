package org.folio.service;

import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import org.folio.rest.jaxrs.model.FileDefinition;

import java.io.InputStream;

/**
 * File storage service. For each implementation should implement this service
 */

public interface FileStorageService {

  /**
   * @return - service name to lookup implementation
   */
  String getServiceName();

  /**
   * Search file at storage
   */
  Future<Buffer> getFile(String path);

  /**
   * Saves File to the storage and return its path
   */
  Future<FileDefinition> saveFile(InputStream data, FileDefinition fileDefinition);

  Future<String> getStoragePath(String uploadDefinitionId);

  /**
   * Delete File
   */
  Future<Boolean> deleteFile(String path);

}
