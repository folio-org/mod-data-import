package org.folio.service.upload;

import io.vertx.core.Future;
import org.folio.rest.jaxrs.model.DefinitionCollection;
import org.folio.rest.jaxrs.model.FileDefinition;
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
  Future<DefinitionCollection> getUploadDefinitions(String query, int offset, int limit);

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
   * @param uploadDefinition UploadDefinition to save
   * @return future with generated id
   */
  Future<UploadDefinition> addUploadDefinition(UploadDefinition uploadDefinition);

  /**
   * Updates UploadDefinition with given id
   *
   * @param uploadDefinition UploadDefinition to update
   * @return future with true is succeeded
   */
  Future<UploadDefinition> updateUploadDefinition(UploadDefinition uploadDefinition);

  /**
   * Deletes UploadDefinition by id
   *
   * @param id UploadDefinition id
   * @return future with true is succeeded
   */
  Future<Boolean> deleteUploadDefinition(String id);

  /**
   * Add File Definition into Upload Definition
   *
   * @param fileDefinition - new file definition to add
   * @return future with updated UploadDefinition
   */
  Future<UploadDefinition> addFileDefinitionToUpload(FileDefinition fileDefinition);

}
