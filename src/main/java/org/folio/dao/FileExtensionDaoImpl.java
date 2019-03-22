package org.folio.dao;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.sql.SQLConnection;
import io.vertx.ext.sql.UpdateResult;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.rest.jaxrs.model.FileExtension;
import org.folio.rest.jaxrs.model.FileExtensionCollection;
import org.folio.rest.persist.Criteria.Criteria;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
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

  private static final Logger LOGGER = LoggerFactory.getLogger(FileExtensionDaoImpl.class);

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
    Future<Results<FileExtension>> future = Future.future();
    try {
      String[] fieldList = {"*"};
      CQLWrapper cql = getCQLWrapper(FILE_EXTENSIONS_TABLE, query, limit, offset);
      pgClientFactory.createInstance(tenantId).get(FILE_EXTENSIONS_TABLE, FileExtension.class, fieldList, cql, true, false, future.completer());
    } catch (Exception e) {
      LOGGER.error("Error while searching for FileExtensions", e);
      future.fail(e);
    }
    return future.map(results -> new FileExtensionCollection()
      .withFileExtensions(results.getResults())
      .withTotalRecords(results.getResultInfo().getTotalRecords()));
  }

  @Override
  public Future<FileExtensionCollection> getAllFileExtensionsFromTable(String tableName, String tenantId) {
    Future<Results<FileExtension>> future = Future.future();
    try {
      pgClientFactory.createInstance(tenantId).get(tableName, FileExtension.class, new Criterion(), true, false, future.completer());
    } catch (Exception e) {
      LOGGER.error("Error while searching for FileExtensions", e);
      future.fail(e);
    }
    return future.map(results -> new FileExtensionCollection()
      .withFileExtensions(results.getResults())
      .withTotalRecords(results.getResultInfo().getTotalRecords()));
  }

  @Override
  public Future<Optional<FileExtension>> getFileExtensionById(String id, String tenantId) {
    return getFileExtensionByField(ID_FIELD, id, tenantId);
  }

  private Future<Optional<FileExtension>> getFileExtensionByField(String fieldName, String fieldValue, String tenantId) {
    Future<Results<FileExtension>> future = Future.future();
    try {
      Criteria crit = constructCriteria(fieldName, fieldValue);
      pgClientFactory.createInstance(tenantId).get(FILE_EXTENSIONS_TABLE, FileExtension.class, new Criterion(crit), true, false, future.completer());
    } catch (Exception e) {
      LOGGER.error("Error querying FileExtensions by {}", fieldName, e);
      future.fail(e);
    }
    return future
      .map(Results::getResults)
      .map(fileExtensions -> fileExtensions.isEmpty() ? Optional.empty() : Optional.of(fileExtensions.get(0)));
  }

  @Override
  public Future<Optional<FileExtension>> getFileExtensionByExtenstion(String extension, String tenantId) {
    return getFileExtensionByField(EXTENSION_FIELD, extension, tenantId);
  }

  @Override
  public Future<String> addFileExtension(FileExtension fileExtension, String tenantId) {
    Future<String> future = Future.future();
    pgClientFactory.createInstance(tenantId).save(FILE_EXTENSIONS_TABLE, fileExtension.getId(), fileExtension, future.completer());
    return future;
  }

  @Override
  public Future<FileExtension> updateFileExtension(FileExtension fileExtension, String tenantId) {
    Future<FileExtension> future = Future.future();
    try {
      Criteria idCrit = constructCriteria(ID_FIELD, fileExtension.getId());
      pgClientFactory.createInstance(tenantId).update(FILE_EXTENSIONS_TABLE, fileExtension, new Criterion(idCrit), true, updateResult -> {
        if (updateResult.failed()) {
          LOGGER.error("Could not update fileExtension with id {}", fileExtension.getId(), updateResult.cause());
          future.fail(updateResult.cause());
        } else if (updateResult.result().getUpdated() != 1) {
          String errorMessage = String.format("FileExtension with id '%s' was not found", fileExtension.getId());
          LOGGER.error(errorMessage);
          future.fail(new NotFoundException(errorMessage));
        } else {
          future.complete(fileExtension);
        }
      });
    } catch (Exception e) {
      LOGGER.error("Error updating fileExtension", e);
      future.fail(e);
    }
    return future;
  }

  @Override
  public Future<Boolean> deleteFileExtension(String id, String tenantId) {
    Future<UpdateResult> future = Future.future();
    pgClientFactory.createInstance(tenantId).delete(FILE_EXTENSIONS_TABLE, id, future.completer());
    return future.map(updateResult -> updateResult.getUpdated() == 1);
  }

  @Override
  public Future<FileExtensionCollection> restoreFileExtensions(String tenantId) {
    Future<FileExtensionCollection> future = Future.future();
    Future<SQLConnection> tx = Future.future(); //NOSONAR
    Future.succeededFuture()
      .compose(v -> {
        pgClientFactory.createInstance(tenantId).startTx(tx.completer());
        return tx;
      }).compose(v -> {
      Future<UpdateResult> deleteFuture = Future.future(); //NOSONAR
      pgClientFactory.createInstance(tenantId).delete(tx, FILE_EXTENSIONS_TABLE, new Criterion(), deleteFuture);
      return deleteFuture;
    }).compose(v -> copyExtensionsFromDefault(tx, tenantId))
      .compose(updateHandler -> {
        if (updateHandler.getUpdated() < 1) {
          throw new InternalServerErrorException();
        }
        Future<Void> endTxFuture = Future.future(); //NOSONAR
        pgClientFactory.createInstance(tenantId).endTx(tx, endTxFuture);
        return endTxFuture;
      }).setHandler(result -> {
      if (result.failed()) {
        pgClientFactory.createInstance(tenantId).rollbackTx(tx, rollback -> future.fail(result.cause()));
      } else {
        future.complete();
      }
    });
    return future.compose(v -> getAllFileExtensionsFromTable(FILE_EXTENSIONS_TABLE, tenantId));
  }

  private Future<UpdateResult> copyExtensionsFromDefault(Future<SQLConnection> tx, String tenantId) {
    String moduleName = PostgresClient.getModuleName();
    Future<UpdateResult> resultFuture = Future.future();
    StringBuilder sqlScript = new StringBuilder("INSERT INTO ")
      .append(tenantId).append("_").append(moduleName).append(".").append(FILE_EXTENSIONS_TABLE)
      .append(" SELECT * FROM ")
      .append(tenantId).append("_").append(moduleName).append(".").append(DEFAULT_FILE_EXTENSIONS_TABLE).append(";");
    pgClientFactory.createInstance(tenantId).execute(tx, sqlScript.toString(), resultFuture);
    return resultFuture;
  }

  @Override
  public Future<UpdateResult> copyExtensionsFromDefault(String tenantId) {
    Future<UpdateResult> resultFuture = Future.future();
    Future<SQLConnection> tx = Future.future(); //NOSONAR
    Future.succeededFuture()
      .compose(v -> {
        pgClientFactory.createInstance(tenantId).startTx(tx.completer());
        return tx;
      }).compose(v -> copyExtensionsFromDefault(tx, tenantId))
      .setHandler(r -> {
        if (r.succeeded()) {
          pgClientFactory.createInstance(tenantId).endTx(tx, end ->
            resultFuture.complete(r.result()));
        } else {
          pgClientFactory.createInstance(tenantId).rollbackTx(tx, rollback -> {
            LOGGER.error("Error during coping file extensions from default table to the main", r.cause());
            resultFuture.fail(r.cause());
          });
        }
      });
    return resultFuture;
  }
}

