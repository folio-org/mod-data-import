package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpStatus;
import org.folio.dataimport.util.ExceptionHelper;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.annotations.Stream;
import org.folio.rest.jaxrs.model.Error;
import org.folio.rest.jaxrs.model.Errors;
import org.folio.rest.jaxrs.model.FileDefinition;
import org.folio.rest.jaxrs.model.FileExtension;
import org.folio.rest.jaxrs.model.ProcessFilesRqDto;
import org.folio.rest.jaxrs.model.UploadDefinition;
import org.folio.rest.jaxrs.resource.DataImport;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.service.file.FileUploadLifecycleService;
import org.folio.service.file.FileUploadLifecycleServiceImpl;
import org.folio.service.fileextension.FileExtensionService;
import org.folio.service.fileextension.FileExtensionServiceImpl;
import org.folio.service.processing.FileProcessor;
import org.folio.service.upload.UploadDefinitionService;
import org.folio.service.upload.UploadDefinitionServiceImpl;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.util.Collections;
import java.util.Map;

public class DataImportImpl implements DataImport {

  private static final Logger LOG = LoggerFactory.getLogger(DataImportImpl.class);

  private static final String FILE_EXTENSION_DUPLICATE_ERROR_CODE = "fileExtension.duplication.invalid";
  private static final String FILE_EXTENSION_INVALID_ERROR_CODE = "fileExtension.extension.invalid";
  private static final String FILE_EXTENSION_VALIDATE_ERROR_MESSAGE = "Failed to validate file extension";
  private static final String FILE_EXTENSION_VALID_REGEXP = "^\\.(\\w+)$";

  private UploadDefinitionService uploadDefinitionService;
  private FileUploadLifecycleService fileService;
  private FileProcessor fileProcessor;
  private Future<UploadDefinition> fileUploadStateFuture;
  private FileExtensionService fileExtensionService;

  public DataImportImpl(Vertx vertx, String tenantId) {
    String calculatedTenantId = TenantTool.calculateTenantId(tenantId);
    this.uploadDefinitionService = new UploadDefinitionServiceImpl(vertx, calculatedTenantId);
    this.fileService = new FileUploadLifecycleServiceImpl(vertx, calculatedTenantId, this.uploadDefinitionService);
    this.fileProcessor = FileProcessor.createProxy(vertx);
    this.fileExtensionService = new FileExtensionServiceImpl(vertx, calculatedTenantId);
  }

  @Override
  public void postDataImportUploadDefinitions(String lang, UploadDefinition entity, Map<String, String> okapiHeaders,
                                              Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(c -> {
      try {
        if (uploadDefinitionService.checkNewUploadDefinition(entity, asyncResultHandler)) {
          OkapiConnectionParams params = new OkapiConnectionParams(okapiHeaders, vertxContext.owner());
          uploadDefinitionService.addUploadDefinition(entity, params)
            .map((Response) PostDataImportUploadDefinitionsResponse
              .respond201WithApplicationJson(entity, PostDataImportUploadDefinitionsResponse.headersFor201()))
            .otherwise(ExceptionHelper::mapExceptionToResponse)
            .setHandler(asyncResultHandler);
        }
      } catch (Exception e) {
        asyncResultHandler.handle(Future.succeededFuture(
          ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getDataImportUploadDefinitions(String query, int offset, int limit, String lang, Map<String, String> okapiHeaders,
                                             Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(c -> {
      try {
        uploadDefinitionService.getUploadDefinitions(query, offset, limit)
          .map(GetDataImportUploadDefinitionsResponse::respond200WithApplicationJson)
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
  public void putDataImportUploadDefinitionsByUploadDefinitionId(String uploadDefinitionId, String lang, UploadDefinition entity,
                                                                 Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
                                                                 Context vertxContext) {
    vertxContext.runOnContext(c -> {
      try {
        entity.setId(uploadDefinitionId);
        uploadDefinitionService.updateBlocking(uploadDefinitionId, uploadDef ->
          // just update UploadDefinition without FileDefinition changes
          Future.succeededFuture(entity.withFileDefinitions(uploadDef.getFileDefinitions())))
          .map(PutDataImportUploadDefinitionsByUploadDefinitionIdResponse::respond200WithApplicationJson)
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
  public void deleteDataImportUploadDefinitionsByUploadDefinitionId(String uploadDefinitionId, String lang, Map<String, String> okapiHeaders,
                                                                    Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(c -> {
      try {
        OkapiConnectionParams params = new OkapiConnectionParams(okapiHeaders, vertxContext.owner());
        uploadDefinitionService.deleteUploadDefinition(uploadDefinitionId, params)
          .map(deleted -> deleted ?
            DeleteDataImportUploadDefinitionsByUploadDefinitionIdResponse.respond204WithTextPlain(
              String.format("Upload definition with id '%s' was successfully deleted", uploadDefinitionId)) :
            buildUploadDefinitionNotFound(uploadDefinitionId))
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        LOG.error("Error while deleting upload definition", e);
        asyncResultHandler.handle(Future.succeededFuture(
          DeleteDataImportUploadDefinitionsByUploadDefinitionIdResponse.
            respond500WithTextPlain(Response.Status.INTERNAL_SERVER_ERROR.getReasonPhrase())));
      }
    });
  }

  @Override
  public void getDataImportUploadDefinitionsByUploadDefinitionId(String uploadDefinitionId, String lang, Map<String, String> okapiHeaders,
                                                                 Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(c -> {
      try {
        uploadDefinitionService.getUploadDefinitionById(uploadDefinitionId)
          .map(optionalDefinition -> optionalDefinition.orElseThrow(() ->
            new NotFoundException(String.format("Upload Definition with id '%s' not found", uploadDefinitionId))))
          .map(GetDataImportUploadDefinitionsByUploadDefinitionIdResponse::respond200WithApplicationJson)
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
  public void postDataImportUploadDefinitionsFilesByUploadDefinitionId(String uploadDefinitionId, FileDefinition entity,
                                                                       Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
                                                                       Context vertxContext) {
    vertxContext.runOnContext(c -> {
      try {
        uploadDefinitionService.addFileDefinitionToUpload(entity)
          .map(PostDataImportUploadDefinitionsFilesByUploadDefinitionIdResponse::respond201WithApplicationJson)
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
  public void deleteDataImportUploadDefinitionsFilesByUploadDefinitionIdAndFileId(String uploadDefinitionId, String fileId, Map<String, String> okapiHeaders,
                                                                                  Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    try {
      OkapiConnectionParams params = new OkapiConnectionParams(okapiHeaders, vertxContext.owner());
      vertxContext.runOnContext(c -> fileService.deleteFile(fileId, uploadDefinitionId, params)
        .map(deleted -> deleted ?
          DeleteDataImportUploadDefinitionsFilesByUploadDefinitionIdAndFileIdResponse.respond204WithTextPlain(
            String.format("File with id: %s deleted", fileId)) :
          buildUploadDefinitionNotFound(uploadDefinitionId)
        )
        .otherwise(ExceptionHelper::mapExceptionToResponse)
        .setHandler(asyncResultHandler));
    } catch (Exception e) {
      LOG.error("Error during file delete", e);
      asyncResultHandler.handle(Future.succeededFuture(
        DeleteDataImportUploadDefinitionsFilesByUploadDefinitionIdAndFileIdResponse.
          respond500WithTextPlain(Response.Status.INTERNAL_SERVER_ERROR.getReasonPhrase())));
    }
  }

  @Stream
  @Override
  public void postDataImportUploadDefinitionsFilesByUploadDefinitionIdAndFileId(String uploadDefinitionId, String fileId,
                                                                                InputStream entity, Map<String, String> okapiHeaders,
                                                                                Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(c -> {
      try {
        byte[] data = IOUtils.toByteArray(entity);
        OkapiConnectionParams params = new OkapiConnectionParams(okapiHeaders, vertxContext.owner());
        if (fileUploadStateFuture == null) {
          fileUploadStateFuture = fileService.beforeFileSave(fileId, uploadDefinitionId, params);
        }
        fileUploadStateFuture = fileUploadStateFuture.compose(def ->
          fileService.saveFileChunk(fileId, def, data, params)
            .compose(fileDefinition -> data.length == 0 ?
              fileService.afterFileSave(fileDefinition, params)
              : Future.succeededFuture(def)));
        fileUploadStateFuture.map(PostDataImportUploadDefinitionsFilesByUploadDefinitionIdAndFileIdResponse::respond200WithApplicationJson)
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
  public void postDataImportUploadDefinitionsProcessFilesByUploadDefinitionId(String uploadDefinitionId, String jobProfileId,
                                                                              ProcessFilesRqDto entity, Map<String, String> okapiHeaders,
                                                                              Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(c -> {
      try {
        LOG.info("Starting file processing for upload definition {}", uploadDefinitionId);
        fileProcessor.process(JsonObject.mapFrom(entity), JsonObject.mapFrom(okapiHeaders));
        Future.succeededFuture()
          .map(PostDataImportUploadDefinitionsProcessFilesByUploadDefinitionIdResponse::respond204WithTextPlain)
          .map(Response.class::cast)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        asyncResultHandler.handle(Future.succeededFuture(
          ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getDataImportFileExtensions(String query, int offset, int limit, String lang, Map<String, String> okapiHeaders,
                                          Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        fileExtensionService.getFileExtensions(query, offset, limit)
          .map(GetDataImportFileExtensionsResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        LOG.error("Failed to get all file extensions", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void postDataImportFileExtensions(String lang, FileExtension entity, Map<String, String> okapiHeaders,
                                           Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        validateFileExtension(entity).setHandler(errors -> {
          if (errors.failed()) {
            LOG.error(FILE_EXTENSION_VALIDATE_ERROR_MESSAGE, errors.cause());
            asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(errors.cause())));
          } else if (errors.result().getTotalRecords() > 0) {
            asyncResultHandler.handle(Future.succeededFuture(PostDataImportFileExtensionsResponse.respond422WithApplicationJson(errors.result())));
          } else {
            fileExtensionService.addFileExtension(entity, new OkapiConnectionParams(okapiHeaders, vertxContext.owner()))
              .map((Response) PostDataImportFileExtensionsResponse
                .respond201WithApplicationJson(entity, PostDataImportFileExtensionsResponse.headersFor201()))
              .otherwise(ExceptionHelper::mapExceptionToResponse)
              .setHandler(asyncResultHandler);
          }
        });
      } catch (Exception e) {
        LOG.error("Failed to create file extension", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getDataImportFileExtensionsById(String id, String lang, Map<String, String> okapiHeaders,
                                              Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(c -> {
      try {
        fileExtensionService.getFileExtensionById(id)
          .map(optionalFileExtension -> optionalFileExtension.orElseThrow(() ->
            new NotFoundException(String.format("FileExtension with id '%s' was not found", id))))
          .map(GetDataImportFileExtensionsByIdResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        LOG.error("Failed to get file extension by id", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void putDataImportFileExtensionsById(String id, String lang, FileExtension entity, Map<String, String> okapiHeaders,
                                              Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        validateFileExtension(entity).setHandler(errors -> {
          entity.setId(id);
          if (errors.failed()) {
            LOG.error(FILE_EXTENSION_VALIDATE_ERROR_MESSAGE, errors.cause());
            asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(errors.cause())));
          } else if (errors.result().getTotalRecords() > 0) {
            asyncResultHandler.handle(Future.succeededFuture(PutDataImportFileExtensionsByIdResponse.respond422WithApplicationJson(errors.result())));
          } else {
            fileExtensionService.updateFileExtension(entity, new OkapiConnectionParams(okapiHeaders, vertxContext.owner()))
              .map(updatedEntity -> (Response) PutDataImportFileExtensionsByIdResponse.respond200WithApplicationJson(updatedEntity))
              .otherwise(ExceptionHelper::mapExceptionToResponse)
              .setHandler(asyncResultHandler);
          }
        });
      } catch (Exception e) {
        LOG.error("Failed to update file extension", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void deleteDataImportFileExtensionsById(String id, String lang, Map<String, String> okapiHeaders,
                                                 Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        fileExtensionService.deleteFileExtension(id)
          .map(deleted -> deleted ?
            DeleteDataImportFileExtensionsByIdResponse.respond204WithTextPlain(
              String.format("FileExtension with id '%s' was successfully deleted", id)) :
            DeleteDataImportFileExtensionsByIdResponse.respond404WithTextPlain(
              String.format("FileExtension with id '%s' was not found", id)))
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        LOG.error("Failed to delete file extension", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void postDataImportFileExtensionsRestoreDefault(Map<String, String> okapiHeaders,
                                                         Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        fileExtensionService.restoreFileExtensions()
          .map(defaultCollection -> (Response) PostDataImportFileExtensionsRestoreDefaultResponse
            .respond200WithApplicationJson(defaultCollection))
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        LOG.error("Failed to restore file extensions", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getDataImportDataTypes(Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
                                     Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        fileExtensionService.getDataTypes()
          .map(GetDataImportDataTypesResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .setHandler(asyncResultHandler);
      } catch (Exception e) {
        LOG.error("Failed to get all data types", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
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

  /**
   * Validate {@link FileExtension} before save or update
   *
   * @param extension - {@link FileExtension} object to create or update
   * @return - Future {@link Errors} object with list of validation errors
   */
  private Future<Errors> validateFileExtension(FileExtension extension) {
    Errors errors = new Errors()
      .withTotalRecords(0);
    return Future.succeededFuture()
      .map(v -> !extension.getExtension().matches(FILE_EXTENSION_VALID_REGEXP)
        ? errors.withErrors(Collections.singletonList(new Error().withMessage(FILE_EXTENSION_INVALID_ERROR_CODE))).withTotalRecords(errors.getErrors().size() + 1)
        : errors)
      .compose(errorsReply -> fileExtensionService.isFileExtensionExistByName(extension))
      .map(exist -> exist && errors.getTotalRecords() == 0
        ? errors.withErrors(Collections.singletonList(new Error().withMessage(FILE_EXTENSION_DUPLICATE_ERROR_CODE))).withTotalRecords(errors.getErrors().size() + 1)
        : errors);
  }
}
