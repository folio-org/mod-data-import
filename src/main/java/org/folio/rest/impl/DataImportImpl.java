package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.http.HttpStatus;
import org.folio.dataImport.util.ExceptionHelper;
import org.folio.dataImport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.FileDefinition;
import org.folio.rest.jaxrs.model.ProcessChunkingRqDto;
import org.folio.rest.jaxrs.model.UploadDefinition;
import org.folio.rest.jaxrs.resource.DataImport;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.service.chunking.FileBlockingChunkingHandlerImpl;
import org.folio.service.chunking.FileChunkingHandler;
import org.folio.service.file.FileService;
import org.folio.service.file.FileServiceImpl;
import org.folio.service.upload.UploadDefinitionService;
import org.folio.service.upload.UploadDefinitionServiceImpl;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.util.Map;

public class DataImportImpl implements DataImport {

  private static final Logger LOG = LoggerFactory.getLogger("mod-data-import");
  private UploadDefinitionService uploadDefinitionService;
  private FileService fileService;
  private FileChunkingHandler fileChunkingHandler;


  public DataImportImpl(Vertx vertx, String tenantId) {
    String calculatedTenantId = TenantTool.calculateTenantId(tenantId);
    this.uploadDefinitionService = new UploadDefinitionServiceImpl(vertx, calculatedTenantId);
    this.fileService = new FileServiceImpl(vertx, calculatedTenantId, this.uploadDefinitionService);
    this.fileChunkingHandler = new FileBlockingChunkingHandlerImpl(vertx);
  }

  @Override
  public void postDataImportUploadDefinition(String lang, UploadDefinition entity, Map<String, String> okapiHeaders,
                                             Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(c -> {
      try {
        OkapiConnectionParams params = new OkapiConnectionParams(okapiHeaders, vertxContext.owner());
        uploadDefinitionService.addUploadDefinition(entity, params)
          .map((Response) PostDataImportUploadDefinitionResponse
            .respond201WithApplicationJson(entity, PostDataImportUploadDefinitionResponse.headersFor201()))
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        asyncResultHandler.handle(Future.succeededFuture(
          ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getDataImportUploadDefinition(String query, int offset, int limit, String lang,
                                            Map<String, String> okapiHeaders,
                                            Handler<AsyncResult<Response>> asyncResultHandler,
                                            Context vertxContext) {
    vertxContext.runOnContext(c -> {
      try {
        uploadDefinitionService.getUploadDefinitions(query, offset, limit)
          .map(GetDataImportUploadDefinitionResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        asyncResultHandler.handle(Future.succeededFuture(
          ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void putDataImportUploadDefinitionByDefinitionId(String definitionId, UploadDefinition entity,
                                                          Map<String, String> okapiHeaders,
                                                          Handler<AsyncResult<Response>> asyncResultHandler,
                                                          Context vertxContext) {
    vertxContext.runOnContext(c -> {
      try {
        entity.setId(definitionId);
        uploadDefinitionService.updateUploadDefinition(entity)
          .map(PutDataImportUploadDefinitionByDefinitionIdResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        asyncResultHandler.handle(Future.succeededFuture(
          ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getDataImportUploadDefinitionByDefinitionId(String definitionId, Map<String, String> okapiHeaders,
                                                          Handler<AsyncResult<Response>> asyncResultHandler,
                                                          Context vertxContext) {
    vertxContext.runOnContext(c -> {
      try {
        uploadDefinitionService.getUploadDefinitionById(definitionId)
          .map(optionalDefinition -> optionalDefinition.orElseThrow(() ->
            new NotFoundException(String.format("Upload Definition with id '%s' not found", definitionId))))
          .map(GetDataImportUploadDefinitionByDefinitionIdResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        asyncResultHandler.handle(Future.succeededFuture(
          ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void deleteDataImportUploadDefinitionByDefinitionId(String definitionId, Map<String, String> okapiHeaders,
                                                             Handler<AsyncResult<Response>> asyncResultHandler,
                                                             Context vertxContext) {
    vertxContext.runOnContext(c -> {
      try {
        OkapiConnectionParams params = new OkapiConnectionParams(okapiHeaders, vertxContext.owner());
        uploadDefinitionService.deleteUploadDefinition(definitionId, params)
          .map(deleted -> deleted ?
            DeleteDataImportUploadDefinitionByDefinitionIdResponse.respond204WithTextPlain(
              String.format("Upload definition with id '%s' was successfully deleted", definitionId)) :
            buildUploadDefinitionNotFound(definitionId))
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        LOG.error("Error while deleting upload definition", e);
        asyncResultHandler.handle(Future.succeededFuture(
          DeleteDataImportUploadDefinitionByDefinitionIdResponse.
            respond500WithTextPlain(Response.Status.INTERNAL_SERVER_ERROR.getReasonPhrase())));
      }
    });
  }

  @Override
  public void postDataImportUploadDefinitionFile(FileDefinition entity, Map<String, String> okapiHeaders,
                                                 Handler<AsyncResult<Response>> asyncResultHandler,
                                                 Context vertxContext) {
    vertxContext.runOnContext(c -> {
      try {
        uploadDefinitionService.addFileDefinitionToUpload(entity)
          .map(PostDataImportUploadDefinitionFileResponse::respond201WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        asyncResultHandler.handle(Future.succeededFuture(
          ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void deleteDataImportUploadDefinitionFileByFileId(String fileId, String uploadDefinitionId,
                                                           Map<String, String> okapiHeaders,
                                                           Handler<AsyncResult<Response>> asyncResultHandler,
                                                           Context vertxContext) {
    try {
      vertxContext.runOnContext(c -> fileService.deleteFile(fileId, uploadDefinitionId)
        .map(deleted -> deleted ?
          DeleteDataImportUploadDefinitionFileByFileIdResponse.respond204WithTextPlain(
            String.format("File with id: %s deleted", fileId)) :
          buildUploadDefinitionNotFound(uploadDefinitionId)
        )
        .otherwise(ExceptionHelper::mapExceptionToResponse)
        .setHandler(asyncResultHandler));
    } catch (Exception e) {
      LOG.error("Error during file delete", e);
      asyncResultHandler.handle(Future.succeededFuture(
        DeleteDataImportUploadDefinitionFileByFileIdResponse.
          respond500WithTextPlain(Response.Status.INTERNAL_SERVER_ERROR.getReasonPhrase())));
    }
  }

  @Override
  public void postDataImportUploadFile(String uploadDefinitionId, String fileId, InputStream entity,
                                       Map<String, String> okapiHeaders,
                                       Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(c -> {
      try {
        OkapiConnectionParams params = new OkapiConnectionParams(okapiHeaders, vertxContext.owner());
        fileService.uploadFile(fileId, uploadDefinitionId, entity, params)
          .map(PostDataImportUploadFileResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        asyncResultHandler.handle(Future.succeededFuture(
          ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }


  @Override
  public void postDataImportProcessFiles(ProcessChunkingRqDto request, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(c -> {
      try {
        OkapiConnectionParams params = new OkapiConnectionParams(okapiHeaders, vertxContext.owner());
        fileChunkingHandler.handle(request.getUploadDefinition(), request.getProfile(), params)
          .map(PostDataImportProcessFilesResponse::respond201WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(DataImportHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        asyncResultHandler.handle(Future.succeededFuture(
          DataImportHelper.mapExceptionToResponse(e)));
      }
    });
  }

  private Response buildUploadDefinitionNotFound(String definitionId) {
    return Response
      .status(HttpStatus.SC_NOT_FOUND)
      .type(MediaType.TEXT_PLAIN)
      .entity(String.format("Upload Definition with id '%s' was not found", definitionId))
      .build();
  }
}
