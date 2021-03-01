package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.rest.jaxrs.model.FileExtension;
import org.folio.rest.jaxrs.model.FileExtensionCollection;
import org.folio.rest.persist.Criteria.Criteria;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.SQLConnection;
import org.folio.rest.persist.cql.CQLWrapper;
import org.folio.rest.persist.interfaces.Results;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotFoundException;
import java.util.Optional;

import static org.folio.dataimport.util.DaoUtil.constructCriteria;
import static org.folio.dataimport.util.DaoUtil.getCQLWrapper;

@Repository
public class FileExtensionDaoImpl implements FileExtensionDao {

  private static final Logger LOGGER = LogManager.getLogger();

  private static final String FILE_EXTENSIONS_TABLE = "file_extensions";
  private static final String DEFAULT_FILE_EXTENSIONS_TABLE = "default_file_extensions";
  private static final String ID_FIELD = "'id'";
  private static final String EXTENSION_FIELD = "'extension'";

  @Autowired
  private PostgresClientFactory pgClientFactory;

  public FileExtensionDaoImpl() {
  }

  /**
   * This constructor is used till {@link org.folio.service.processing.ParallelFileChunkingProcessor}
   * will be rewritten with DI support.
   *
   * @param vertx
   */
  public FileExtensionDaoImpl(Vertx vertx) {
    pgClientFactory = new PostgresClientFactory(vertx);
  }

  @Override
  public Future<FileExtensionCollection> getFileExtensions(String query, int offset, int limit, String tenantId) {
    Promise<Results<FileExtension>> promise = Promise.promise();
    try {
      String[] fieldList = {"*"};
      CQLWrapper cql = getCQLWrapper(FILE_EXTENSIONS_TABLE, query, limit, offset);
      pgClientFactory.createInstance(tenantId).get(FILE_EXTENSIONS_TABLE, FileExtension.class, fieldList, cql, true, false, promise);
    } catch (Exception e) {
      LOGGER.error("Error while searching for FileExtensions", e);
      promise.fail(e);
    }
    return promise.future().map(results -> new FileExtensionCollection()
      .withFileExtensions(results.getResults())
      .withTotalRecords(results.getResultInfo().getTotalRecords()));
  }

  @Override
  public Future<FileExtensionCollection> getAllFileExtensionsFromTable(String tableName, String tenantId) {
    Promise<Results<FileExtension>> promise = Promise.promise();
    try {
      pgClientFactory.createInstance(tenantId).get(tableName, FileExtension.class, new Criterion(), true, false, promise);
    } catch (Exception e) {
      LOGGER.error("Error while searching for FileExtensions", e);
      promise.fail(e);
    }
    return promise.future().map(results -> new FileExtensionCollection()
      .withFileExtensions(results.getResults())
      .withTotalRecords(results.getResultInfo().getTotalRecords()));
  }

  @Override
  public Future<Optional<FileExtension>> getFileExtensionById(String id, String tenantId) {
    return getFileExtensionByField(ID_FIELD, id, tenantId);
  }

  private Future<Optional<FileExtension>> getFileExtensionByField(String fieldName, String fieldValue, String tenantId) {
    Promise<Results<FileExtension>> promise = Promise.promise();
    try {
      Criteria crit = constructCriteria(fieldName, fieldValue);
      pgClientFactory.createInstance(tenantId).get(FILE_EXTENSIONS_TABLE, FileExtension.class, new Criterion(crit), true, false, promise);
    } catch (Exception e) {
      LOGGER.error("Error querying FileExtensions by {}", fieldName, e);
      promise.fail(e);
    }
    return promise.future()
      .map(Results::getResults)
      .map(fileExtensions -> fileExtensions.isEmpty() ? Optional.empty() : Optional.of(fileExtensions.get(0)));
  }

  @Override
  public Future<Optional<FileExtension>> getFileExtensionByExtenstion(String extension, String tenantId) {
    return getFileExtensionByField(EXTENSION_FIELD, extension, tenantId);
  }

  @Override
  public Future<String> addFileExtension(FileExtension fileExtension, String tenantId) {
    Promise<String> promise = Promise.promise();
    pgClientFactory.createInstance(tenantId).save(FILE_EXTENSIONS_TABLE, fileExtension.getId(), fileExtension, promise);
    return promise.future();
  }

  @Override
  public Future<FileExtension> updateFileExtension(FileExtension fileExtension, String tenantId) {
    Promise<FileExtension> promise = Promise.promise();
    try {
      Criteria idCrit = constructCriteria(ID_FIELD, fileExtension.getId());
      pgClientFactory.createInstance(tenantId).update(FILE_EXTENSIONS_TABLE, fileExtension, new Criterion(idCrit), true, updateResult -> {
        if (updateResult.failed()) {
          LOGGER.error("Could not update fileExtension with id {}", fileExtension.getId(), updateResult.cause());
          promise.fail(updateResult.cause());
        } else if (updateResult.result().rowCount() != 1) {
          String errorMessage = String.format("FileExtension with id '%s' was not found", fileExtension.getId());
          LOGGER.error(errorMessage);
          promise.fail(new NotFoundException(errorMessage));
        } else {
          promise.complete(fileExtension);
        }
      });
    } catch (Exception e) {
      LOGGER.error("Error updating fileExtension", e);
      promise.fail(e);
    }
    return promise.future();
  }

  @Override
  public Future<Boolean> deleteFileExtension(String id, String tenantId) {
    Promise<RowSet<Row>> promise = Promise.promise();
    pgClientFactory.createInstance(tenantId).delete(FILE_EXTENSIONS_TABLE, id, promise);
    return promise.future().map(updateResult -> updateResult.rowCount() == 1);
  }

  @Override
  public Future<FileExtensionCollection> restoreFileExtensions(String tenantId) {
    PostgresClient client = pgClientFactory.createInstance(tenantId);
    Promise<FileExtensionCollection> promise = Promise.promise();
    Promise<SQLConnection> tx = Promise.promise();
    Future.succeededFuture()
      .compose(v -> {
        client.startTx(tx);
        return tx.future();
      }).compose(v -> {
      Promise<RowSet<Row>> deletePromise = Promise.promise();
      client.delete(tx.future(), FILE_EXTENSIONS_TABLE, new Criterion(), deletePromise);
      return deletePromise.future();
    }).compose(v -> copyExtensionsFromDefault(tx.future(), tenantId))
      .compose(updateHandler -> {
        if (updateHandler.rowCount() < 1) {
          throw new InternalServerErrorException();
        }
        Promise<Void> endPromise = Promise.promise();
        client.endTx(tx.future(), endPromise);
        return endPromise.future();
      }).onComplete(result -> {
      if (result.failed()) {
        client.rollbackTx(tx.future(), rollback -> promise.fail(result.cause()));
      } else {
        promise.complete();
      }
    });
    return promise.future()
      .compose(v -> getAllFileExtensionsFromTable(FILE_EXTENSIONS_TABLE, tenantId));
  }

  private Future<RowSet<Row>> copyExtensionsFromDefault(Future<SQLConnection> tx, String tenantId) {
    String moduleName = PostgresClient.getModuleName();
    Promise<RowSet<Row>> promise = Promise.promise();
    StringBuilder sqlScript = new StringBuilder("INSERT INTO ")
      .append(tenantId).append("_").append(moduleName).append(".").append(FILE_EXTENSIONS_TABLE)
      .append(" SELECT * FROM ")
      .append(tenantId).append("_").append(moduleName).append(".").append(DEFAULT_FILE_EXTENSIONS_TABLE).append(";");
    pgClientFactory.createInstance(tenantId).execute(tx, sqlScript.toString(), promise);
    return promise.future();
  }

  @Override
  public Future<RowSet<Row>> copyExtensionsFromDefault(String tenantId) {
    PostgresClient client = pgClientFactory.createInstance(tenantId);
    Promise<RowSet<Row>> promise = Promise.promise();
    Promise<SQLConnection> tx = Promise.promise();
    Future.succeededFuture()
      .compose(v -> {
        client.startTx(tx);
        return tx.future();
      }).compose(v -> copyExtensionsFromDefault(tx.future(), tenantId))
      .onComplete(r -> {
        if (r.succeeded()) {
          client.endTx(tx.future(), end ->
            promise.complete(r.result()));
        } else {
          client.rollbackTx(tx.future(), rollback -> {
            LOGGER.error("Error during coping file extensions from default table to the main", r.cause());
            promise.fail(r.cause());
          });
        }
      });
    return promise.future();
  }
}

