package org.folio.service;

import io.vertx.core.Future;

import java.io.File;

/**
 * File storage service. For each implementation should implement this service
 */

public interface FileStorageService {

  /**
   * Search file at storage
   */
  Future<File> getFile(String path);


  /**
   * Saves File to the storage and return its path
   */
  Future<String> saveFile(File file);


  /**
   * Delete File
   */
  Future<Boolean> deleteFile(String path);

}
