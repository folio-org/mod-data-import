package org.folio.service.upload;

import io.vertx.core.Future;
import org.folio.dao.UploadDefinitionDaoImpl;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.DefinitionCollection;
import org.folio.rest.jaxrs.model.Errors;
import org.folio.rest.jaxrs.model.FileDefinition;
import org.folio.rest.jaxrs.model.StatusDto;
import org.folio.rest.jaxrs.model.UploadDefinition;

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
  Future<UploadDefinition> addUploadDefinition(UploadDefinition uploadDefinition, OkapiConnectionParams params);

  /**
   * Updates {@link UploadDefinition} in database with row blocking
   *
   * @param uploadDefinitionId - id of {@link UploadDefinition}
   * @param mutator            - callback for change {@link UploadDefinition} before save
   * @return - future with updated {@link UploadDefinition}
   */
  Future<UploadDefinition> updateBlocking(String uploadDefinitionId, UploadDefinitionDaoImpl.UploadDefinitionMutator mutator);

  /**
   * Deletes UploadDefinition by id
   *
   * @param id UploadDefinition id
   * @return future with true if succeeded
   */
  Future<Boolean> deleteUploadDefinition(String id, OkapiConnectionParams params);

  /**
   * Add File Definition into Upload Definition
   *
   * @param fileDefinition - new file definition to add
   * @return future with updated UploadDefinition
   */
  Future<UploadDefinition> addFileDefinitionToUpload(FileDefinition fileDefinition);

  /**
   * Updates JobExecution status
   *
   * @param jobExecutionId JobExecution id
   * @param status         new status
   * @param params         OKAPI connection parameters
   * @return future with true if succeeded
   */
  Future<Boolean> updateJobExecutionStatus(String jobExecutionId, StatusDto status, OkapiConnectionParams params);

  /**
   * Deletes file from storage
   *
   * @param fileDefinition FileDefinition
   * @param params         OKAPI connection parameters
   * @return future with true if succeeded
   */
  Future<Boolean> deleteFile(FileDefinition fileDefinition, OkapiConnectionParams params);

  /**
   * Validate new UploadDefinition before saving it
   *
   * @param definition - object with new upload definition
   * @param -          request's result handler
   * @return - {@link Errors} object with errors. Valid UploadDefinition if errors count is zero
   */
  Future<Errors> checkNewUploadDefinition(UploadDefinition definition);

}
