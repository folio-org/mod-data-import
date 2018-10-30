package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.sql.UpdateResult;
import org.folio.rest.jaxrs.model.UploadDefinition;
import org.folio.rest.persist.Criteria.Criteria;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.Criteria.Limit;
import org.folio.rest.persist.Criteria.Offset;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.cql.CQLWrapper;
import org.folio.rest.persist.interfaces.Results;
import org.z3950.zing.cql.cql2pgjson.CQL2PgJSON;

import java.util.List;
import java.util.Optional;

public class UploadDefinitionDaoImpl implements UploadDefinitionDao {

  public static final String UPLOAD_DEFINITION_SCHEMA_PATH = "ramls/uploadDefinition.json";
  private static final String UPLOAD_DEFINITION_TABLE = "uploadDefinition";
  private static final String UPLOAD_DEFINITION_ID_FIELD = "'id'";

  private PostgresClient pgClient;

  public UploadDefinitionDaoImpl(Vertx vertx, String tenantId) {
    pgClient = PostgresClient.getInstance(vertx, tenantId);
  }

  @Override
  public Future<List<UploadDefinition>> getUploadDefinitions(String query, int offset, int limit) {
    Future<Results<UploadDefinition>> future = Future.future();
    try {
      String[] fieldList = {"*"};
      CQLWrapper cql = getCQL(query, limit, offset);
      pgClient.get(UPLOAD_DEFINITION_TABLE, UploadDefinition.class, fieldList, cql, true, false, future.completer());
    } catch (Exception e) {
      future.fail(e);
    }
    return future.map(Results::getResults);
  }

  @Override
  public Future<Optional<UploadDefinition>> getUploadDefinitionById(String id) {
    Future<Results<UploadDefinition>> future = Future.future();
    try {
      Criteria idCrit = new Criteria(UPLOAD_DEFINITION_SCHEMA_PATH);
      idCrit.addField(UPLOAD_DEFINITION_ID_FIELD);
      idCrit.setOperation("=");
      idCrit.setValue(id);
      pgClient.get(UPLOAD_DEFINITION_TABLE, UploadDefinition.class, new Criterion(idCrit), true, future.completer());
    } catch (Exception e) {
      future.fail(e);
    }
    return future
      .map(Results::getResults)
      .map(uploadDefinitions -> uploadDefinitions.isEmpty() ? Optional.empty() : Optional.of(uploadDefinitions.get(0)));
  }

  @Override
  public Future<String> addUploadDefinition(UploadDefinition uploadDefinition) {
    Future<String> future = Future.future();
    pgClient.save(UPLOAD_DEFINITION_TABLE, uploadDefinition.getId(), uploadDefinition, future.completer());
    return future;
  }

  @Override
  public Future<Boolean> updateUploadDefinition(UploadDefinition uploadDefinition) {
    Future<UpdateResult> future = Future.future();
    try {
      Criteria idCrit = new Criteria();
      idCrit.addField(UPLOAD_DEFINITION_ID_FIELD);
      idCrit.setOperation("=");
      idCrit.setValue(uploadDefinition.getId());
      pgClient.update(UPLOAD_DEFINITION_TABLE, uploadDefinition, new Criterion(idCrit), true, future.completer());
    } catch (Exception e) {
      future.fail(e);
    }
    return future.map(updateResult -> updateResult.getUpdated() == 1);
  }

  @Override
  public Future<Boolean> deleteUploadDefinition(String id) {
    Future<UpdateResult> future = Future.future();
    pgClient.delete(UPLOAD_DEFINITION_TABLE, id, future.completer());
    return future.map(updateResult -> updateResult.getUpdated() == 1);
  }

  /**
   * Build CQL from request URL query
   *
   * @param query - query from URL
   * @param limit - limit of records for pagination
   * @return - CQL wrapper for building postgres request to database
   * @throws org.z3950.zing.cql.cql2pgjson.FieldException field exception
   */
  private CQLWrapper getCQL(String query, int limit, int offset)
    throws org.z3950.zing.cql.cql2pgjson.FieldException {
    CQL2PgJSON cql2pgJson = new CQL2PgJSON(UPLOAD_DEFINITION_TABLE + ".jsonb");
    return new CQLWrapper(cql2pgJson, query)
      .setLimit(new Limit(limit))
      .setOffset(new Offset(offset));
  }
}
