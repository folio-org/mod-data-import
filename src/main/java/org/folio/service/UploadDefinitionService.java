package org.folio.service;

import io.vertx.core.Future;
import org.folio.rest.jaxrs.model.UploadDefinition;

import java.util.List;
import java.util.Optional;

/**
 * UploadDefinition service
 */

public interface UploadDefinitionService {

  /**
   * Searches for UploadDefinitions
   *
   * @param query  CQL query
   * @param offset offset
   * @param limit  limit
   * @return future with list of UploadDefinitions
   */
  Future<List<UploadDefinition>> getUploadDefinitions(String query, int offset, int limit);

  /**
   * Searches for UploadDefinition by id
   *
   * @param id UploadDefinition id
   * @return future with optional UploadDefinition
   */
  Future<Optional<UploadDefinition>> getUploadDefinitionById(String id);

  /**
   * Saves UploadDefinition with generated id
   *
   * @param UploadDefinition UploadDefinition to save
   * @return future with generated id
   */
  Future<String> addUploadDefinition(UploadDefinition UploadDefinition);

  /**
   * Updates UploadDefinition with given id
   *
   * @param UploadDefinition UploadDefinition to update
   * @return future with true is succeeded
   */
  Future<Boolean> updateUploadDefinition(UploadDefinition UploadDefinition);

  /**
   * Deletes UploadDefinition by id
   *
   * @param id UploadDefinition id
   * @return future with true is succeeded
   */
  Future<Boolean> deleteUploadDefinition(String id);

}
