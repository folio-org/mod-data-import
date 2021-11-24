package org.folio.service.upload;

import io.vertx.core.Future;
import org.folio.dao.UploadDefinitionDaoImpl;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.DefinitionCollection;
import org.folio.rest.jaxrs.model.Errors;
import org.folio.rest.jaxrs.model.FileDefinition;
import org.folio.rest.jaxrs.model.JobExecutionDto;
import org.folio.rest.jaxrs.model.StatusDto;
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
   * @param query    CQL query
   * @param offset   offset
   * @param limit    limit
   * @param tenantId tenant id
   * @return future with list of UploadDefinitions
   */
  Future<DefinitionCollection> getUploadDefinitions(String query, int offset, int limit, String tenantId);

  /**
   * Searches for UploadDefinition by id
   *
   * @param id       UploadDefinition id
   * @param tenantId tenant id
   * @return future with optional UploadDefinition
   */
  Future<Optional<UploadDefinition>> getUploadDefinitionById(String id, String tenantId);

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
   * @param tenantId           tenant id
   * @return - future with updated {@link UploadDefinition}
   */
  Future<UploadDefinition> updateBlocking(String uploadDefinitionId, UploadDefinitionDaoImpl.UploadDefinitionMutator mutator, String tenantId);

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
   * @param tenantId       tenant id
   * @return future with updated UploadDefinition
   */
  Future<UploadDefinition> addFileDefinitionToUpload(FileDefinition fileDefinition, String tenantId);

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
   * @param tenantId   tenant id
   * @return - {@link Errors} object with errors. Valid UploadDefinition if errors count is zero
   */
  Future<Errors> checkNewUploadDefinition(UploadDefinition definition, String tenantId);

  /**
   * Returns job executions by given upload definition
   *
   * @param uploadDefinition given upload definition, which jobs the method returns
   * @param params           OKAPI connection parameters
   * @return future with list of job executions
   */
  Future<List<JobExecutionDto>> getJobExecutions(UploadDefinition uploadDefinition, OkapiConnectionParams params);

  /**
   * Updates {@link FileDefinition} status by specified FileDefinition id
   *
   * @param uploadDefinitionId UploadDefinition id
   * @param fileDefinitionId   FileDefinition id
   * @param status             FileDefinition status
   * @param tenantId           tenant id
   * @return - future with {@link UploadDefinition} which contains updated {@link FileDefinition}
   */
  Future<UploadDefinition> updateFileDefinitionStatus(String uploadDefinitionId, String fileDefinitionId, FileDefinition.Status status, String tenantId);

  /**
   * Updates UploadDefinition status
   *
   * @param uploadDefinitionId UploadDefinition id
   * @param status             UploadDefinition status
   * @param tenantId           tenant id
   * @return - future with updated {@link UploadDefinition}
   */
  Future<UploadDefinition> updateUploadDefinitionStatus(String uploadDefinitionId, UploadDefinition.Status status, String tenantId);
}
