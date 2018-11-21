package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.sql.UpdateResult;
import org.folio.rest.jaxrs.model.DefinitionCollection;
import org.folio.rest.jaxrs.model.UploadDefinition;
import org.folio.rest.persist.Criteria.Criteria;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.Criteria.Limit;
import org.folio.rest.persist.Criteria.Offset;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.cql.CQLWrapper;
import org.folio.rest.persist.interfaces.Results;
import org.z3950.zing.cql.cql2pgjson.CQL2PgJSON;

import javax.ws.rs.NotFoundException;
import java.util.Optional;

public class UploadDefinitionDaoImpl implements UploadDefinitionDao {

  private static final String UPLOAD_DEFINITION_TABLE = "uploadDefinition";
  private static final String UPLOAD_DEFINITION_ID_FIELD = "'id'";
  private final Logger logger = LoggerFactory.getLogger(UploadDefinitionDaoImpl.class);

  private PostgresClient pgClient;
  private String schema;

  public UploadDefinitionDaoImpl(Vertx vertx, String tenantId) {
    pgClient = PostgresClient.getInstance(vertx, tenantId);
    this.schema = PostgresClient.convertToPsqlStandard(tenantId);
  }

  public interface UploadDefinitionMutator {
    Future<UploadDefinition> mutate(UploadDefinition definition);
  }

  public Future<UploadDefinition> updateBlocking(String uploadDefinitionId, UploadDefinitionMutator mutator) {
    Future<UploadDefinition> future = Future.future();
    String rollbackMessage = "Rollback transaction. Error during upload definition update. uploadDefinitionId" + uploadDefinitionId;
    pgClient.startTx(tx -> {
      try {
        StringBuilder selectUploadDefinitionQuery = new StringBuilder("SELECT jsonb FROM ")
          .append(schema)
          .append(".")
          .append(UPLOAD_DEFINITION_TABLE)
          .append(" WHERE _id ='")
          .append(uploadDefinitionId).append("' LIMIT 1 FOR UPDATE;");
        pgClient.execute(tx, selectUploadDefinitionQuery.toString(), selectResult -> {
          if (selectResult.failed() || selectResult.result().getUpdated() != 1) {
            pgClient.rollbackTx(tx, r -> {
              logger.error(rollbackMessage, selectResult.cause());
              future.fail(new NotFoundException(rollbackMessage));
            });
          } else {
            Criteria idCrit = new Criteria();
            idCrit.addField(UPLOAD_DEFINITION_ID_FIELD);
            idCrit.setOperation("=");
            idCrit.setValue(uploadDefinitionId);
            pgClient.get(tx, UPLOAD_DEFINITION_TABLE, UploadDefinition.class, new Criterion(idCrit), false, true, uploadDefResult -> {
              if (uploadDefResult.failed()
                || uploadDefResult.result() == null
                || uploadDefResult.result().getResultInfo() == null
                || uploadDefResult.result().getResultInfo().getTotalRecords() < 1) {
                pgClient.rollbackTx(tx, r -> {
                  logger.error(rollbackMessage);
                  future.fail(new NotFoundException(rollbackMessage));
                });
              } else {
                try {
                  UploadDefinition definition = uploadDefResult.result().getResults().get(0);
                  mutator.mutate(definition)
                    .setHandler(onMutate -> {
                      if (onMutate.succeeded()) {
                        try {
                          CQLWrapper filter = new CQLWrapper(new CQL2PgJSON(UPLOAD_DEFINITION_TABLE + ".jsonb"), "id==" + definition.getId());
                          pgClient.update(tx, UPLOAD_DEFINITION_TABLE, onMutate.result(), filter, true, updateHandler -> {
                            if (updateHandler.succeeded() && updateHandler.result().getUpdated() == 1) {
                              pgClient.endTx(tx, endTx -> {
                                if (endTx.succeeded()) {
                                  future.complete(definition);
                                } else {
                                  logger.error(rollbackMessage);
                                  future.fail("Error during updating UploadDefinition with id: " + uploadDefinitionId);
                                }
                              });
                            } else {
                              pgClient.rollbackTx(tx, r -> {
                                logger.error(rollbackMessage, updateHandler.cause());
                                future.fail(updateHandler.cause());
                              });
                            }
                          });
                        } catch (Exception e) {
                          pgClient.rollbackTx(tx, r -> {
                            logger.error(rollbackMessage, e);
                            future.fail(e);
                          });
                        }
                      } else {
                        pgClient.rollbackTx(tx, r -> {
                          logger.error(rollbackMessage, onMutate.cause());
                          future.fail(onMutate.cause());
                        });
                      }
                    });
                } catch (Exception e) {
                  pgClient.rollbackTx(tx, r -> {
                    logger.error(rollbackMessage, e);
                    future.fail(e);
                  });
                }
              }
            });
          }
        });
      } catch (Exception e) {
        pgClient.rollbackTx(tx, r -> {
          logger.error(rollbackMessage, e);
          future.fail(e);
        });
      }
    });
    return future;
  }

  @Override
  public Future<DefinitionCollection> getUploadDefinitions(String query, int offset, int limit) {
    Future<Results<UploadDefinition>> future = Future.future();
    try {
      String[] fieldList = {"*"};
      CQLWrapper cql = getCQL(query, limit, offset);
      pgClient.get(UPLOAD_DEFINITION_TABLE, UploadDefinition.class, fieldList, cql, true, false, future.completer());
    } catch (Exception e) {
      logger.error("Error during getting UploadDefinitions from view", e);
      future.fail(e);
    }
    return future.map(uploadDefinitionResults -> new DefinitionCollection()
      .withUploadDefinitions(uploadDefinitionResults.getResults())
      .withTotalRecords(uploadDefinitionResults.getResultInfo().getTotalRecords()));
  }

  @Override
  public Future<Optional<UploadDefinition>> getUploadDefinitionById(String id) {
    Future<Results<UploadDefinition>> future = Future.future();
    try {
      Criteria idCrit = new Criteria();
      idCrit.addField(UPLOAD_DEFINITION_ID_FIELD);
      idCrit.setOperation("=");
      idCrit.setValue(id);
      pgClient.get(UPLOAD_DEFINITION_TABLE, UploadDefinition.class, new Criterion(idCrit), true, future.completer());
    } catch (Exception e) {
      logger.error("Error during get UploadDefinition by ID from view", e);
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
      logger.error("Error during updating UploadDefinition by ID", e);
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
