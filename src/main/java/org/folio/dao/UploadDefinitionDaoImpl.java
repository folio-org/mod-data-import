package org.folio.dao;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.sql.ResultSet;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.UpdateResult;
import org.apache.commons.lang3.time.TimeZones;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.rest.jaxrs.model.DefinitionCollection;
import org.folio.rest.jaxrs.model.UploadDefinition;
import org.folio.rest.jaxrs.model.UploadDefinition.Status;
import org.folio.rest.persist.Criteria.Criteria;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.cql.CQLWrapper;
import org.folio.rest.persist.interfaces.Results;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;
import org.z3950.zing.cql.cql2pgjson.CQL2PgJSON;

import javax.ws.rs.NotFoundException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Optional;
import java.util.TimeZone;

import static org.folio.dataimport.util.DaoUtil.constructCriteria;
import static org.folio.dataimport.util.DaoUtil.getCQLWrapper;

@Repository
public class UploadDefinitionDaoImpl implements UploadDefinitionDao {

  private static final String UPLOAD_DEFINITION_TABLE = "upload_definitions";
  private static final String UPLOAD_DEFINITION_ID_FIELD = "'id'";

  private final Logger logger = LoggerFactory.getLogger(UploadDefinitionDaoImpl.class);
  private final SimpleDateFormat dateFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

  @Autowired
  private PostgresClientFactory pgClientFactory;

  public UploadDefinitionDaoImpl() {
  }

  /**
   * This constructor is used till {@link org.folio.service.processing.ParallelFileChunkingProcessor}
   * will be rewritten with DI support.
   *
   * @param vertx
   */
  public UploadDefinitionDaoImpl(Vertx vertx) {
    pgClientFactory = new PostgresClientFactory(vertx);
  }

  /**
   * Functional interface for change UploadDefinition in blocking update statement
   */
  @FunctionalInterface
  public interface UploadDefinitionMutator {
    /**
     * @param definition - Loaded from DB UploadDefinition
     * @return - changed Upload Definition ready for save into database
     */
    Future<UploadDefinition> mutate(UploadDefinition definition);
  }

  public Future<UploadDefinition> updateBlocking(String uploadDefinitionId, UploadDefinitionMutator mutator, String tenantId) {
    Future<UploadDefinition> future = Future.future();
    String rollbackMessage = "Rollback transaction. Error during upload definition update. uploadDefinitionId " + uploadDefinitionId; //NOSONAR
    Future<SQLConnection> tx = Future.future(); //NOSONAR
    Future.succeededFuture()
      .compose(v -> {
        pgClientFactory.createInstance(tenantId).startTx(tx.completer());
        return tx;
      }).compose(v -> {
      Future<ResultSet> selectFuture = Future.future(); //NOSONAR
      StringBuilder selectUploadDefinitionQuery = new StringBuilder("SELECT jsonb FROM ") //NOSONAR
        .append(PostgresClient.convertToPsqlStandard(tenantId))
        .append(".")
        .append(UPLOAD_DEFINITION_TABLE)
        .append(" WHERE _id ='")
        .append(uploadDefinitionId).append("' LIMIT 1 FOR UPDATE;");
      pgClientFactory.createInstance(tenantId).select(tx, selectUploadDefinitionQuery.toString(), selectFuture);
      return selectFuture;
    }).compose(resultSet -> {
      if (resultSet.getNumRows() != 1) {
        throw new NotFoundException("Upload Definition was not found. ID: " + uploadDefinitionId);
      }
      UploadDefinition definition = new JsonObject(resultSet.getRows().get(0).getString("jsonb")) //NOSONAR
        .mapTo(UploadDefinition.class);
      return mutator.mutate(definition);
    }).compose(mutatedObject -> updateUploadDefinition(tx, mutatedObject, tenantId))
      .setHandler(onUpdate -> {
        if (onUpdate.succeeded()) {
          pgClientFactory.createInstance(tenantId).endTx(tx, endTx ->
            future.complete(onUpdate.result()));
        } else {
          pgClientFactory.createInstance(tenantId).rollbackTx(tx, r -> {
            logger.error(rollbackMessage, onUpdate.cause());
            future.fail(onUpdate.cause());
          });
        }
      });
    return future;
  }

  @Override
  public Future<DefinitionCollection> getUploadDefinitions(String query, int offset, int limit, String tenantId) {
    Future<Results<UploadDefinition>> future = Future.future();
    try {
      String[] fieldList = {"*"};
      CQLWrapper cql = getCQLWrapper(UPLOAD_DEFINITION_TABLE, query, limit, offset);
      pgClientFactory.createInstance(tenantId).get(UPLOAD_DEFINITION_TABLE, UploadDefinition.class, fieldList, cql, true, false, future.completer());
    } catch (Exception e) {
      logger.error("Error during getting UploadDefinitions from view", e);
      future.fail(e);
    }
    return future.map(uploadDefinitionResults -> new DefinitionCollection()
      .withUploadDefinitions(uploadDefinitionResults.getResults())
      .withTotalRecords(uploadDefinitionResults.getResultInfo().getTotalRecords()));
  }

  @Override
  public Future<Optional<UploadDefinition>> getUploadDefinitionById(String id, String tenantId) {
    Future<Results<UploadDefinition>> future = Future.future();
    try {
      Criteria idCrit = constructCriteria(UPLOAD_DEFINITION_ID_FIELD, id);
      pgClientFactory.createInstance(tenantId).get(UPLOAD_DEFINITION_TABLE, UploadDefinition.class, new Criterion(idCrit), true, future.completer());
    } catch (Exception e) {
      logger.error("Error during get UploadDefinition by ID from view", e);
      future.fail(e);
    }
    return future
      .map(Results::getResults)
      .map(uploadDefinitions -> uploadDefinitions.isEmpty() ? Optional.empty() : Optional.of(uploadDefinitions.get(0)));
  }

  @Override
  public Future<String> addUploadDefinition(UploadDefinition uploadDefinition, String tenantId) {
    Future<String> future = Future.future();
    pgClientFactory.createInstance(tenantId).save(UPLOAD_DEFINITION_TABLE, uploadDefinition.getId(), uploadDefinition, future.completer());
    return future;
  }

  @Override
  public Future<UploadDefinition> updateUploadDefinition(AsyncResult<SQLConnection> tx, UploadDefinition uploadDefinition, String tenantId) {
    Future<UpdateResult> future = Future.future();
    try {
      CQLWrapper filter = new CQLWrapper(new CQL2PgJSON(UPLOAD_DEFINITION_TABLE + ".jsonb"), "id==" + uploadDefinition.getId());
      pgClientFactory.createInstance(tenantId).update(tx, UPLOAD_DEFINITION_TABLE, uploadDefinition, filter, true, future.completer());
    } catch (Exception e) {
      logger.error("Error during updating UploadDefinition by ID", e);
      future.fail(e);
    }
    return future.map(uploadDefinition);
  }

  @Override
  public Future<Boolean> deleteUploadDefinition(String id, String tenantId) {
    Future<UpdateResult> future = Future.future();
    pgClientFactory.createInstance(tenantId).delete(UPLOAD_DEFINITION_TABLE, id, future.completer());
    return future.map(updateResult -> updateResult.getUpdated() == 1);
  }

  @Override
  public Future<DefinitionCollection> getUploadDefinitionsByStatusOrUpdatedDateNotGreaterThen(Status status, Date lastUpdateDate, int offset, int limit, String tenantId) {
    Future<Results<UploadDefinition>> future = Future.future();
    try {
      String[] fieldList = {"*"};
      String queryFilter = getFilterByStatus(status, lastUpdateDate);
      pgClientFactory.createInstance(tenantId).get(UPLOAD_DEFINITION_TABLE, UploadDefinition.class, fieldList, queryFilter, true, false, future.completer());
    } catch (Exception e) {
      logger.error("Error during getting UploadDefinitions by status and date", e);
      future.fail(e);
    }
    return future.map(uploadDefinitionResults -> new DefinitionCollection()
      .withUploadDefinitions(uploadDefinitionResults.getResults())
      .withTotalRecords(uploadDefinitionResults.getResultInfo().getTotalRecords()));
  }

  private String getFilterByStatus(Status status, Date date) {
    String queryFilterTemplate = "WHERE %s.jsonb ->> 'status' = '%s' OR TO_TIMESTAMP(%s.jsonb -> 'metadata' ->> 'updatedDate', 'YYYY-MM-DD HH24:MI:SS') <= '%s'";
    dateFormatter.setTimeZone(TimeZone.getTimeZone(TimeZones.GMT_ID));
    return String.format(queryFilterTemplate, UPLOAD_DEFINITION_TABLE, status.toString(), UPLOAD_DEFINITION_TABLE, dateFormatter.format(date));
  }
}
