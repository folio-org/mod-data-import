package org.folio.service.storage;

import io.vertx.core.Future;
import java.io.File;
import org.folio.dataimport.util.ConnectionParams;
import org.folio.rest.jaxrs.model.FileDefinition;

/**
 * File storage service. For each implementation should implement this service.
 */

public interface FileStorageService {

  /**
   * Return service name to lookup implementation.
   */
  String getServiceName();

  /**
   * Search file at storage.
   */
  File getFile(String path);

  /**
   * Saves File to the storage and return its path.
   */
  Future<FileDefinition> saveFile(byte[] data, FileDefinition fileDefinition, ConnectionParams params);

  /**
   * Deletes File from the storage and returns true if succeeded.
   */
  Future<Boolean> deleteFile(FileDefinition fileDefinition);
}
