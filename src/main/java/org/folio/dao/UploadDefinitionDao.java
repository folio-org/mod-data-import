package org.folio.dao;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.ext.sql.SQLConnection;
import org.folio.rest.jaxrs.model.DefinitionCollection;
import org.folio.rest.jaxrs.model.UploadDefinition;

import java.util.Optional;

/**
 * Data access object for {@link UploadDefinition}
 */
public interface UploadDefinitionDao {

  /**
   * Searches for {@link UploadDefinition} in database
   *
   * @param query  CQL query
   * @param offset offset
   * @param limit  limit
   * @param tenantId tenant id
   * @return future with list of {@link UploadDefinition}
   */
  Future<DefinitionCollection> getUploadDefinitions(String query, int offset, int limit, String tenantId);

  /**
   * Searches for {@link UploadDefinition} by id
   *
   * @param id UploadDefinition id
   * @param tenantId tenant id
   * @return future with optional {@link UploadDefinition}
   */
  Future<Optional<UploadDefinition>> getUploadDefinitionById(String id, String tenantId);

  /**
   * Saves {@link UploadDefinition} to database
   *
   * @param uploadDefinition {@link UploadDefinition} to save
   * @param tenantId tenant id
   * @return future with id of saved {@link UploadDefinition}
   */
  Future<String> addUploadDefinition(UploadDefinition uploadDefinition, String tenantId);

  /**
   * Updates {@link UploadDefinition} in database in transaction
   *
   * @param tx               {@link SQLConnection} transaction's connection
   * @param uploadDefinition {@link UploadDefinition} to update
   * @param tenantId tenant id
   * @return future with updated {@link UploadDefinition}
   */
  Future<UploadDefinition> updateUploadDefinition(AsyncResult<SQLConnection> tx, UploadDefinition uploadDefinition, String tenantId);

  /**
   * Updates {@link UploadDefinition} in database with row blocking
   *
   * @param uploadDefinitionId - id of {@link UploadDefinition}
   * @param mutator            - callback for change {@link UploadDefinition} before save
   * @param tenantId tenant id
   * @return - future with updated {@link UploadDefinition}
   */
  Future<UploadDefinition> updateBlocking(String uploadDefinitionId, UploadDefinitionDaoImpl.UploadDefinitionMutator mutator, String tenantId);

  /**
   * Deletes {@link UploadDefinition} from database
   *
   * @param id id of {@link UploadDefinition} to delete
   * @param tenantId tenant id
   * @return future with true is succeeded
   */
  Future<Boolean> deleteUploadDefinition(String id, String tenantId);
}
