package org.folio.dao;

import io.vertx.core.Future;
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
   * @return future with list of {@link UploadDefinition}
   */
  Future<DefinitionCollection> getUploadDefinitions(String query, int offset, int limit);

  /**
   * Searches for {@link UploadDefinition} by id
   *
   * @param id UploadDefinition id
   * @return future with optional {@link UploadDefinition}
   */
  Future<Optional<UploadDefinition>> getUploadDefinitionById(String id);

  /**
   * Saves {@link UploadDefinition} to database
   *
   * @param uploadDefinition {@link UploadDefinition} to save
   * @return future with id of saved {@link UploadDefinition}
   */
  Future<String> addUploadDefinition(UploadDefinition uploadDefinition);

  /**
   * Updates {@link UploadDefinition} in database
   *
   * @param uploadDefinition {@link UploadDefinition} to update
   * @return future with true is succeeded
   */
  Future<Boolean> updateUploadDefinition(UploadDefinition uploadDefinition);

  /**
   * Deletes {@link UploadDefinition} from database
   *
   * @param id id of {@link UploadDefinition} to delete
   * @return future with true is succeeded
   */
  Future<Boolean> deleteUploadDefinition(String id);
}
