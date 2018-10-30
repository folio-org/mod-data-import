package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.sql.UpdateResult;
import org.folio.rest.jaxrs.model.File;
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

public class FileDaoImpl implements FileDao {

  public static final String FILE_SCHEMA_PATH = "ramls/file.json";
  private static final String FILE_TABLE = "file";
  private static final String FILE_ID_FIELD = "'id'";

  private PostgresClient pgClient;

  public FileDaoImpl(Vertx vertx, String tenantId) {
    pgClient = PostgresClient.getInstance(vertx, tenantId);
  }

  @Override
  public Future<List<File>> getFiles(String query, int offset, int limit) {
    Future<Results<File>> future = Future.future();
    try {
      String[] fieldList = {"*"};
      CQLWrapper cql = getCQL(query, limit, offset);
      pgClient.get(FILE_TABLE, File.class, fieldList, cql, true, false, future.completer());
    } catch (Exception e) {
      future.fail(e);
    }
    return future.map(Results::getResults);
  }

  @Override
  public Future<Optional<File>> getFileById(String id) {
    Future<Results<File>> future = Future.future();
    try {
      Criteria idCrit = new Criteria(FILE_SCHEMA_PATH);
      idCrit.addField(FILE_ID_FIELD);
      idCrit.setOperation("=");
      idCrit.setValue(id);
      pgClient.get(FILE_TABLE, File.class, new Criterion(idCrit), true, future.completer());
    } catch (Exception e) {
      future.fail(e);
    }
    return future
      .map(Results::getResults)
      .map(Files -> Files.isEmpty() ? Optional.empty() : Optional.of(Files.get(0)));
  }

  @Override
  public Future<String> addFile(File file) {
    Future<String> future = Future.future();
    pgClient.save(FILE_TABLE, file.getId(), file, future.completer());
    return future;
  }

  @Override
  public Future<Boolean> updateFile(File file) {
    Future<UpdateResult> future = Future.future();
    try {
      Criteria idCrit = new Criteria();
      idCrit.addField(FILE_ID_FIELD);
      idCrit.setOperation("=");
      idCrit.setValue(file.getId());
      pgClient.update(FILE_TABLE, file, new Criterion(idCrit), true, future.completer());
    } catch (Exception e) {
      future.fail(e);
    }
    return future.map(updateResult -> updateResult.getUpdated() == 1);
  }

  @Override
  public Future<Boolean> deleteFile(String id) {
    Future<UpdateResult> future = Future.future();
    pgClient.delete(FILE_TABLE, id, future.completer());
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
    CQL2PgJSON cql2pgJson = new CQL2PgJSON(FILE_TABLE + ".jsonb");
    return new CQLWrapper(cql2pgJson, query)
      .setLimit(new Limit(limit))
      .setOffset(new Offset(offset));
  }
}
