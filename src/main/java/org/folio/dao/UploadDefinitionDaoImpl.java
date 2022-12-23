package org.folio.dao;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;

import org.apache.commons.lang3.time.TimeZones;
import org.folio.cql2pgjson.CQL2PgJSON;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.rest.jaxrs.model.DefinitionCollection;
import org.folio.rest.jaxrs.model.UploadDefinition;
import org.folio.rest.jaxrs.model.UploadDefinition.Status;
import org.folio.rest.persist.Criteria.Criteria;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.SQLConnection;
import org.folio.rest.persist.cql.CQLWrapper;
import org.folio.rest.persist.interfaces.Results;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import javax.ws.rs.NotFoundException;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Optional;
import java.util.TimeZone;

import static org.folio.dataimport.util.DaoUtil.constructCriteria;
import static org.folio.dataimport.util.DaoUtil.getCQLWrapper;
import static org.folio.rest.jaxrs.model.UploadDefinition.Status.COMPLETED;

@Repository
public class UploadDefinitionDaoImpl implements UploadDefinitionDao {

  private static final String UPLOAD_DEFINITION_TABLE = "upload_definitions";
  private static final String UPLOAD_DEFINITION_ID_FIELD = "id";
  private static final String STATUS_FIELD = "'status'";
  private static final String METADATA_FIELD = "'metadata'";
  private static final String UPDATED_DATE_FIELD = "'updatedDate'";
  private static final String DATE_FORMAT_PATTERN = "yyyy-MM-dd'T'HH:mm:ss.SSSX";

  private static final Logger LOGGER = LogManager.getLogger();
  private SimpleDateFormat dateFormatter;

  @Autowired
  private PostgresClientFactory pgClientFactory;

  public UploadDefinitionDaoImpl() {
    dateFormatter = new SimpleDateFormat(DATE_FORMAT_PATTERN);
    dateFormatter.setTimeZone(TimeZone.getTimeZone(TimeZones.GMT_ID));
  }

  /**
   * This constructor is used till {@link org.folio.service.processing.ParallelFileChunkingProcessor}
   * will be rewritten with DI support.
   *
   * @param vertx
   */
  public UploadDefinitionDaoImpl(Vertx vertx) {
    super();
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
    PostgresClient client = pgClientFactory.createInstance(tenantId);
    Promise<UploadDefinition> promise = Promise.promise();
    String rollbackMessage = "Rollback transaction. Error during upload definition update. uploadDefinitionId " + uploadDefinitionId;
    Promise<SQLConnection> tx = Promise.promise();
    Future.succeededFuture()
      .compose(v -> {
        client.startTx(tx);
        return tx.future();
      }).compose(v -> {
      Promise<RowSet<Row>> selectPromise = Promise.promise();
      StringBuilder selectUploadDefinitionQuery = new StringBuilder("SELECT jsonb FROM ")
        .append(PostgresClient.convertToPsqlStandard(tenantId))
        .append(".")
        .append(UPLOAD_DEFINITION_TABLE)
        .append(" WHERE id ='")
        .append(uploadDefinitionId).append("' LIMIT 1 FOR UPDATE;");
      client.select(tx.future(), selectUploadDefinitionQuery.toString(), selectPromise);
      return selectPromise.future();
    }).compose(resultSet -> {
      if (resultSet.rowCount() != 1) {
        throw new NotFoundException("Upload Definition was not found. ID: " + uploadDefinitionId);
      }
      UploadDefinition definition = new JsonObject(resultSet.iterator().next().getValue("jsonb").toString())
        .mapTo(UploadDefinition.class);
      return mutator.mutate(definition);
    }).compose(mutatedObject -> updateUploadDefinition(tx.future(), mutatedObject, tenantId))
      .onComplete(onUpdate -> {
        if (onUpdate.succeeded()) {
          client.endTx(tx.future(), endTx ->
            promise.complete(onUpdate.result()));
        } else {
          client.rollbackTx(tx.future(), r -> {
            LOGGER.warn(rollbackMessage, onUpdate.cause());
            promise.fail(onUpdate.cause());
          });
        }
      });
    return promise.future();
  }

  @Override
  public Future<DefinitionCollection> getUploadDefinitions(String query, int offset, int limit, String tenantId) {
    Promise<Results<UploadDefinition>> promise = Promise.promise();
    try {
      String[] fieldList = {"*"};
      CQLWrapper cql = getCQLWrapper(UPLOAD_DEFINITION_TABLE, query, limit, offset);
      pgClientFactory.createInstance(tenantId).get(UPLOAD_DEFINITION_TABLE, UploadDefinition.class, fieldList, cql, true, false, promise);
    } catch (Exception e) {
      LOGGER.warn("getUploadDefinitions:: Error during getting UploadDefinitions from view", e);
      promise.fail(e);
    }
    return promise.future().map(uploadDefinitionResults -> new DefinitionCollection()
      .withUploadDefinitions(uploadDefinitionResults.getResults())
      .withTotalRecords(uploadDefinitionResults.getResultInfo().getTotalRecords()));
  }

  @Override
  public Future<Optional<UploadDefinition>> getUploadDefinitionById(String id, String tenantId) {
    Promise<Results<UploadDefinition>> promise = Promise.promise();
    try {
      Criteria idCrit = constructCriteria(UPLOAD_DEFINITION_ID_FIELD, id).setJSONB(false);
      pgClientFactory.createInstance(tenantId).get(UPLOAD_DEFINITION_TABLE, UploadDefinition.class, new Criterion(idCrit), false, promise);
    } catch (Exception e) {
      LOGGER.warn("getUploadDefinitionById:: Error during get UploadDefinition by ID from view", e);
      promise.fail(e);
    }
    return promise.future()
      .map(Results::getResults)
      .map(uploadDefinitions -> uploadDefinitions.isEmpty() ? Optional.empty() : Optional.of(uploadDefinitions.get(0)));
  }

  @Override
  public Future<String> addUploadDefinition(UploadDefinition uploadDefinition, String tenantId) {
    LOGGER.debug("addUploadDefinition:: adding upload definition {} for tenant {}", uploadDefinition.getId(), tenantId);
    Promise<String> promise = Promise.promise();
    pgClientFactory.createInstance(tenantId).save(UPLOAD_DEFINITION_TABLE, uploadDefinition.getId(), uploadDefinition, promise);
    return promise.future();
  }

  @Override
  public Future<UploadDefinition> updateUploadDefinition(AsyncResult<SQLConnection> tx, UploadDefinition uploadDefinition, String tenantId) {
    LOGGER.debug("updateUploadDefinition:: updating upload definition with id {} for tenant {}", uploadDefinition.getId(), tenantId);
    Promise<RowSet<Row>> promise = Promise.promise();
    try {
      CQLWrapper filter = new CQLWrapper(new CQL2PgJSON(UPLOAD_DEFINITION_TABLE + ".jsonb"), "id==" + uploadDefinition.getId());
      pgClientFactory.createInstance(tenantId).update(tx, UPLOAD_DEFINITION_TABLE, uploadDefinition, filter, true, promise);
    } catch (Exception e) {
      LOGGER.warn("updateUploadDefinition:: Error during updating UploadDefinition by ID", e);
      promise.fail(e);
    }
    return promise.future().map(uploadDefinition);
  }

  @Override
  public Future<Boolean> deleteUploadDefinition(String id, String tenantId) {
    LOGGER.debug("deleteUploadDefinition:: delete upload definition with id {} for tenant {}", id, tenantId);
    Promise<RowSet<Row>> promise = Promise.promise();
    pgClientFactory.createInstance(tenantId).delete(UPLOAD_DEFINITION_TABLE, id, promise);
    return promise.future().map(updateResult -> updateResult.rowCount() == 1);
  }

  @Override
  public Future<DefinitionCollection> getUploadDefinitionsByStatusOrUpdatedDateNotGreaterThen(Status status, Date lastUpdateDate, int offset, int limit, String tenantId) {
    Promise<Results<UploadDefinition>> promise = Promise.promise();
    try {
      Criterion filter = getFilterByStatus(status, lastUpdateDate);
      pgClientFactory.createInstance(tenantId).get(UPLOAD_DEFINITION_TABLE, UploadDefinition.class, filter, true, promise);
    } catch (Exception e) {
      LOGGER.warn("getUploadDefinitionsByStatusOrUpdatedDateNotGreaterThen:: Error during getting UploadDefinitions by status and date", e);
      promise.fail(e);
    }
    return promise.future().map(uploadDefinitionResults -> new DefinitionCollection()
      .withUploadDefinitions(uploadDefinitionResults.getResults())
      .withTotalRecords(uploadDefinitionResults.getResultInfo().getTotalRecords()));
  }

  private Criterion getFilterByStatus(Status status, Date date) {
    Criteria updatedDateCriteria = new Criteria()
      .addField(METADATA_FIELD)
      .addField(UPDATED_DATE_FIELD)
      .setOperation("<=")
      .setVal(dateFormatter.format(date));

    return new Criterion(constructCriteria(STATUS_FIELD, COMPLETED.value()))
      .addCriterion(updatedDateCriteria, "OR");
  }
}
