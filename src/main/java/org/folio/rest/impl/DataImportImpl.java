package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.rest.jaxrs.model.DataImportUploadFilePostMultipartFormData;
import org.folio.rest.jaxrs.model.DefinitionCollection;
import org.folio.rest.jaxrs.model.UploadDefinition;
import org.folio.rest.jaxrs.resource.DataImport;

import javax.ws.rs.core.Response;
import java.util.Map;

public class DataImportImpl implements DataImport {

  private static final Logger LOG = LoggerFactory.getLogger("mod-data-import");

  @Override
  public void postDataImportUploadDefinition(String lang, UploadDefinition entity, Map<String, String> okapiHeaders,
                                             Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    try {
      vertxContext.runOnContext((c) -> {
        asyncResultHandler.handle(Future.succeededFuture(
          PostDataImportUploadDefinitionResponse
            .respond201WithApplicationJson(entity, PostDataImportUploadDefinitionResponse.headersFor201())));
      });
    } catch (Exception e) {
      LOG.error("Error during create new upload definition", e);
      asyncResultHandler.handle(Future.succeededFuture(
        PostDataImportUploadDefinitionResponse.
          respond500WithTextPlain(Response.Status.INTERNAL_SERVER_ERROR.getReasonPhrase())));
    }
  }

  @Override
  public void getDataImportUploadDefinition(String query, int offset, int limit, String lang,
                                            Map<String, String> okapiHeaders,
                                            Handler<AsyncResult<Response>> asyncResultHandler,
                                            Context vertxContext) {
    try {
      vertxContext.runOnContext((c) -> {
        DefinitionCollection collection = new DefinitionCollection();
        collection.setTotalRecords(0);
        asyncResultHandler.handle(Future.succeededFuture(
          GetDataImportUploadDefinitionResponse
            .respond200WithApplicationJson(collection)));
      });
    } catch (Exception e) {
      LOG.error("Error during lookup upload definitions", e);
      asyncResultHandler.handle(Future.succeededFuture(
        GetDataImportUploadDefinitionResponse.
          respond500WithTextPlain(Response.Status.INTERNAL_SERVER_ERROR.getReasonPhrase())));
    }
  }

  @Override
  public void putDataImportUploadDefinitionByDefinitionId(String definitionId, UploadDefinition entity,
                                                          Map<String, String> okapiHeaders,
                                                          Handler<AsyncResult<Response>> asyncResultHandler,
                                                          Context vertxContext) {
    try {
      vertxContext.runOnContext((c) -> {
        asyncResultHandler.handle(Future.succeededFuture(
          PutDataImportUploadDefinitionByDefinitionIdResponse
            .respond200WithApplicationJson(entity)));
      });
    } catch (Exception e) {
      LOG.error("Error during update upload definitions", e);
      asyncResultHandler.handle(Future.succeededFuture(
        PutDataImportUploadDefinitionByDefinitionIdResponse.
          respond500WithTextPlain(Response.Status.INTERNAL_SERVER_ERROR.getReasonPhrase())));
    }
  }

  @Override
  public void getDataImportUploadDefinitionByDefinitionId(String definitionId, Map<String, String> okapiHeaders,
                                                          Handler<AsyncResult<Response>> asyncResultHandler,
                                                          Context vertxContext) {
    try {
      vertxContext.runOnContext((c) -> {
        asyncResultHandler.handle(Future.succeededFuture(
          GetDataImportUploadDefinitionByDefinitionIdResponse
            .respond200WithApplicationJson(new UploadDefinition())));
      });
    } catch (Exception e) {
      LOG.error("Error during lookup upload definitions", e);
      asyncResultHandler.handle(Future.succeededFuture(
        GetDataImportUploadDefinitionByDefinitionIdResponse.
          respond500WithTextPlain(Response.Status.INTERNAL_SERVER_ERROR.getReasonPhrase())));
    }
  }

  @Override
  public void postDataImportUploadFile(String jobExecutionId, String uploadDefinitionId,
                                       DataImportUploadFilePostMultipartFormData entity, Map<String, String> okapiHeaders,
                                       Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    try {
      vertxContext.runOnContext((c) -> {
        asyncResultHandler.handle(Future.succeededFuture(
          PostDataImportUploadFileResponse
            .respond201WithApplicationJson(new UploadDefinition())));
      });
    } catch (Exception e) {
      LOG.error("Error during file upload", e);
      asyncResultHandler.handle(Future.succeededFuture(
        PostDataImportUploadFileResponse.
          respond500WithTextPlain(Response.Status.INTERNAL_SERVER_ERROR.getReasonPhrase())));
    }
  }


  @Override
  public void deleteDataImportUploadFileByFileId(String fileId, Map<String, String> okapiHeaders,
                                                 Handler<AsyncResult<Response>> asyncResultHandler,
                                                 Context vertxContext) {
    try {
      vertxContext.runOnContext((c) -> {
        asyncResultHandler.handle(Future.succeededFuture(
          DeleteDataImportUploadFileByFileIdResponse
            .respond204WithTextPlain("File was successfully deleted")));
      });
    } catch (Exception e) {
      LOG.error("Error during file delete", e);
      asyncResultHandler.handle(Future.succeededFuture(
        DeleteDataImportUploadFileByFileIdResponse.
          respond500WithTextPlain(Response.Status.INTERNAL_SERVER_ERROR.getReasonPhrase())));
    }
  }
}
