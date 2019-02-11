package org.folio.service.storage;

import io.vertx.core.Future;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.FileDefinition;

import java.io.File;

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
  File getFile(String path);

  /**
   * Saves File to the storage and return its path
   */
  Future<FileDefinition> saveFile(byte[] data, FileDefinition fileDefinition, OkapiConnectionParams params);

  /**
   * Deletes File from the storage and returns true if succeeded
   */
  Future<Boolean> deleteFile(FileDefinition fileDefinition);

}
