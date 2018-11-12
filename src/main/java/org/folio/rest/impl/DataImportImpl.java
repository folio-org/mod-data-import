package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.http.HttpStatus;
import org.folio.rest.jaxrs.model.DefinitionCollection;
import org.folio.rest.jaxrs.model.UploadDefinition;
import org.folio.rest.jaxrs.resource.DataImport;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.service.FileService;
import org.folio.service.FileServiceImpl;
import org.folio.service.UploadDefinitionService;
import org.folio.service.UploadDefinitionServiceImpl;
import org.folio.util.DataImportHelper;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.util.Map;

public class DataImportImpl implements DataImport {

  private static final Logger LOG = LoggerFactory.getLogger("mod-data-import");
  private UploadDefinitionService uploadDefinitionService;
  private FileService fileService;

  public DataImportImpl(Vertx vertx, String tenantId) {
    String calculatedTenantId = TenantTool.calculateTenantId(tenantId);
    this.uploadDefinitionService = new UploadDefinitionServiceImpl(vertx, calculatedTenantId);
    this.fileService = new FileServiceImpl(vertx, calculatedTenantId);
  }

  @Override
  public void postDataImportUploadDefinition(String lang, UploadDefinition entity, Map<String, String> okapiHeaders,
                                             Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(c -> {
      try {
        uploadDefinitionService.addUploadDefinition(entity)
          .map((Response) PostDataImportUploadDefinitionResponse
            .respond201WithApplicationJson(entity, PostDataImportUploadDefinitionResponse.headersFor201()))
          .otherwise(DataImportHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        asyncResultHandler.handle(Future.succeededFuture(
          DataImportHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getDataImportUploadDefinition(String query, int offset, int limit, String lang,
                                            Map<String, String> okapiHeaders,
                                            Handler<AsyncResult<Response>> asyncResultHandler,
                                            Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        uploadDefinitionService.getUploadDefinitions(query, offset, limit)
          .map(definitions -> new DefinitionCollection()
            .withUploadDefinitions(definitions)
            .withTotalRecords(definitions.size())
          ).map(GetDataImportUploadDefinitionResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(DataImportHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        asyncResultHandler.handle(Future.succeededFuture(
          DataImportHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void putDataImportUploadDefinitionByDefinitionId(String definitionId, UploadDefinition entity,
                                                          Map<String, String> okapiHeaders,
                                                          Handler<AsyncResult<Response>> asyncResultHandler,
                                                          Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        entity.setId(definitionId);
        uploadDefinitionService.updateUploadDefinition(entity)
          .map(PutDataImportUploadDefinitionByDefinitionIdResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(DataImportHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        asyncResultHandler.handle(Future.succeededFuture(
          DataImportHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getDataImportUploadDefinitionByDefinitionId(String definitionId, Map<String, String> okapiHeaders,
                                                          Handler<AsyncResult<Response>> asyncResultHandler,
                                                          Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        uploadDefinitionService.getUploadDefinitionById(definitionId)
          .map(optionalDefinition -> optionalDefinition.orElseThrow(() ->
            new NotFoundException(String.format("Upload Definition with id '%s' not found", definitionId))))
          .map(GetDataImportUploadDefinitionByDefinitionIdResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(DataImportHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        asyncResultHandler.handle(Future.succeededFuture(
          DataImportHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void postDataImportUploadFile(String uploadDefinitionId, String fileId, InputStream entity,
                                       Map<String, String> okapiHeaders,
                                       Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(c -> {
      try {
        fileService.uploadFile(fileId, uploadDefinitionId, entity)
          .map(PostDataImportUploadFileResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(DataImportHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        asyncResultHandler.handle(Future.succeededFuture(
          DataImportHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void deleteDataImportUploadFileByFileId(String fileId, String uploadDefinitionId,
                                                 Map<String, String> okapiHeaders,
                                                 Handler<AsyncResult<Response>> asyncResultHandler,
                                                 Context vertxContext) {
    try {
      vertxContext.runOnContext(c -> {
        fileService.deleteFile(fileId, uploadDefinitionId)
          .map(deleted -> deleted ?
            DeleteDataImportUploadFileByFileIdResponse.respond204WithTextPlain(
              String.format("File with id: %s deleted", fileId)) :
            buildUploadDefinitionNotFound(uploadDefinitionId)
          )
          .otherwise(DataImportHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      });
    } catch (Exception e) {
      LOG.error("Error during file delete", e);
      asyncResultHandler.handle(Future.succeededFuture(
        DeleteDataImportUploadFileByFileIdResponse.
          respond500WithTextPlain(Response.Status.INTERNAL_SERVER_ERROR.getReasonPhrase())));
    }
  }

  private Response buildUploadDefinitionNotFound(String definitionId) {
    return Response
      .status(HttpStatus.SC_NOT_FOUND)
      .type(MediaType.TEXT_PLAIN)
      .entity(String.format("Upload Definition with id '%s' not found", definitionId))
      .build();
  }
}
