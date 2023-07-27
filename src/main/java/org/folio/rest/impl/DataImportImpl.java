package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.folio.dataimport.util.ExceptionHelper;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.RestVerticle;
import org.folio.rest.annotations.Stream;
import org.folio.rest.jaxrs.model.AssembleFileDto;
import org.folio.rest.jaxrs.model.Error;
import org.folio.rest.jaxrs.model.Errors;
import org.folio.rest.jaxrs.model.FileDefinition;
import org.folio.rest.jaxrs.model.FileExtension;
import org.folio.rest.jaxrs.model.ProcessFilesRqDto;
import org.folio.rest.jaxrs.model.SplitStatus;
import org.folio.rest.jaxrs.model.UploadDefinition;
import org.folio.rest.jaxrs.resource.DataImport;
import org.folio.rest.tools.utils.TenantTool;
import org.folio.service.file.FileUploadLifecycleService;
import org.folio.service.fileextension.FileExtensionService;
import org.folio.service.processing.FileProcessor;
import org.folio.service.s3storage.MinioStorageService;
import org.folio.service.upload.UploadDefinitionService;
import org.folio.spring.SpringContextUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import javax.ws.rs.NotFoundException;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.Map;

import static org.folio.rest.RestVerticle.OKAPI_USERID_HEADER;
import static org.folio.rest.RestVerticle.STREAM_ABORT;
import static org.folio.rest.jaxrs.model.FileDefinition.Status.ERROR;

public class DataImportImpl implements DataImport {

  private static final Logger LOGGER = LogManager.getLogger();

  private static final String FILE_EXTENSION_DUPLICATE_ERROR_CODE = "fileExtension.duplication.invalid";
  private static final String FILE_EXTENSION_INVALID_ERROR_CODE = "fileExtension.extension.invalid";
  private static final String FILE_EXTENSION_VALIDATE_ERROR_MESSAGE = "Failed to validate file extension";
  private static final String UPLOAD_DEFINITION_VALIDATE_ERROR_MESSAGE = "Failed to validate Upload Definition";
  private static final String FILE_EXTENSION_VALID_REGEXP = "^\\.(\\w+)$";

  @Autowired
  private UploadDefinitionService uploadDefinitionService;
  @Autowired
  private FileUploadLifecycleService fileService;
  @Autowired
  private FileExtensionService fileExtensionService;

  @Autowired
  private MinioStorageService minioStorageService;
  
  @Value("${splitFileProcess:false}")
  private boolean splitFileProcess;

  private final FileProcessor fileProcessor;
  private Future<UploadDefinition> fileUploadStateFuture;
  private final String tenantId;

  public DataImportImpl(Vertx vertx, String tenantId) {
    SpringContextUtil.autowireDependencies(this, Vertx.currentContext());
    this.tenantId = TenantTool.calculateTenantId(tenantId);
    this.fileProcessor = FileProcessor.createProxy(vertx);
  }

  @Override
  public void postDataImportUploadDefinitions(String lang, UploadDefinition entity, Map<String, String> okapiHeaders,
                                              Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(c -> {
      try {
        LOGGER.debug("postDataImportUploadDefinitions:: uploadDefinitionId {}, tenantId {}", entity.getId(), tenantId);
        uploadDefinitionService.checkNewUploadDefinition(entity, tenantId).onComplete(errors -> {
          if (errors.failed()) {
            LOGGER.warn(UPLOAD_DEFINITION_VALIDATE_ERROR_MESSAGE, errors.cause());
            asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(errors.cause())));
          } else if (errors.result().getTotalRecords() > 0) {
            asyncResultHandler.handle(Future.succeededFuture(PostDataImportUploadDefinitionsResponse.respond422WithApplicationJson(errors.result())));
          } else {
            OkapiConnectionParams params = new OkapiConnectionParams(okapiHeaders, vertxContext.owner());
            uploadDefinitionService.addUploadDefinition(entity, params)
              .map((Response) PostDataImportUploadDefinitionsResponse
                .respond201WithApplicationJson(entity, PostDataImportUploadDefinitionsResponse.headersFor201()))
              .otherwise(ExceptionHelper::mapExceptionToResponse)
              .onComplete(asyncResultHandler);
          }
        });
      } catch (Exception e) {
        LOGGER.warn("postDataImportUploadDefinitions:: Cannot create upload definition with id {}", entity.getId(), e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getDataImportUploadDefinitions(String query, int offset, int limit, String lang, Map<String, String> okapiHeaders,
                                             Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(c -> {
      try {
        LOGGER.debug("getDataImportUploadDefinitions:: query {}", query);
        String preparedQuery = addCreatedByConditionToCqlQuery(query, okapiHeaders);
        uploadDefinitionService.getUploadDefinitions(preparedQuery, offset, limit, tenantId)
          .map(GetDataImportUploadDefinitionsResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOGGER.warn("getDataImportUploadDefinitions:: Cannot get upload definitions by query {}", query, e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
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
        LOGGER.debug("putDataImportUploadDefinitionsByUploadDefinitionId:: uploadDefinitionId {}", uploadDefinitionId);
        uploadDefinitionService.updateBlocking(uploadDefinitionId, uploadDef ->
          // just update UploadDefinition without FileDefinition changes
          Future.succeededFuture(entity.withFileDefinitions(uploadDef.getFileDefinitions())), tenantId)
          .map(PutDataImportUploadDefinitionsByUploadDefinitionIdResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOGGER.warn("putDataImportUploadDefinitionsByUploadDefinitionId:: Cannot update upload definition by id {}", uploadDefinitionId, e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void deleteDataImportUploadDefinitionsByUploadDefinitionId(String uploadDefinitionId, String lang, Map<String, String> okapiHeaders,
                                                                    Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(c -> {
      try {
        LOGGER.debug("deleteDataImportUploadDefinitionsByUploadDefinitionId:: uploadDefinitionId {}", uploadDefinitionId);
        OkapiConnectionParams params = new OkapiConnectionParams(okapiHeaders, vertxContext.owner());
        uploadDefinitionService.deleteUploadDefinition(uploadDefinitionId, params)
          .map(deleted -> (Response) DeleteDataImportUploadDefinitionsByUploadDefinitionIdResponse.respond204())
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOGGER.warn("deleteDataImportUploadDefinitionsByUploadDefinitionId:: Error while deleting upload definition", e);
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
        LOGGER.debug("getDataImportUploadDefinitionsByUploadDefinitionId:: uploadDefinitionId {}", uploadDefinitionId);
        uploadDefinitionService.getUploadDefinitionById(uploadDefinitionId, tenantId)
          .map(optionalDefinition -> optionalDefinition.orElseThrow(() ->
            new NotFoundException(String.format("Upload Definition with id '%s' not found", uploadDefinitionId))))
          .map(GetDataImportUploadDefinitionsByUploadDefinitionIdResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOGGER.warn("getDataImportUploadDefinitionsByUploadDefinitionId:: Cannot get upload definitions by id {}", uploadDefinitionId, e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void postDataImportUploadDefinitionsFilesByUploadDefinitionId(String uploadDefinitionId, FileDefinition entity,
                                                                       Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
                                                                       Context vertxContext) {
    vertxContext.runOnContext(c -> {
      try {
        LOGGER.debug("postDataImportUploadDefinitionsFilesByUploadDefinitionId:: uploadDefinitionId {}, fileDefinitionId {}", uploadDefinitionId, entity.getId());
        uploadDefinitionService.addFileDefinitionToUpload(entity, tenantId)
          .map(PostDataImportUploadDefinitionsFilesByUploadDefinitionIdResponse::respond201WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOGGER.warn("postDataImportUploadDefinitionsFilesByUploadDefinitionId:: Cannot create upload definitions files by uploadDefinitionId {}", uploadDefinitionId, e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void deleteDataImportUploadDefinitionsFilesByUploadDefinitionIdAndFileId(String uploadDefinitionId, String fileId, Map<String, String> okapiHeaders,
                                                                                  Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    try {
      LOGGER.debug("deleteDataImportUploadDefinitionsFilesByUploadDefinitionIdAndFileId:: fileId {}, uploadDefinitionId {}", fileId, uploadDefinitionId);
      OkapiConnectionParams params = new OkapiConnectionParams(okapiHeaders, vertxContext.owner());
      vertxContext.runOnContext(c -> fileService.deleteFile(fileId, uploadDefinitionId, params)
        .map(deleted -> (Response) DeleteDataImportUploadDefinitionsFilesByUploadDefinitionIdAndFileIdResponse.respond204())
        .otherwise(ExceptionHelper::mapExceptionToResponse)
        .onComplete(asyncResultHandler));
    } catch (Exception e) {
      LOGGER.warn("deleteDataImportUploadDefinitionsFilesByUploadDefinitionIdAndFileId:: Error during file delete", e);
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
    try {
      LOGGER.debug("postDataImportUploadDefinitionsFilesByUploadDefinitionIdAndFileId:: uploadDefinitionId {}, fileId {}", uploadDefinitionId, fileId);
      Future<Response> responseFuture;
      OkapiConnectionParams params = new OkapiConnectionParams(okapiHeaders, vertxContext.owner());
      if (okapiHeaders.get(STREAM_ABORT) == null) {
        byte[] data = IOUtils.toByteArray(entity);
        if (fileUploadStateFuture == null) {
          fileUploadStateFuture = fileService.beforeFileSave(fileId, uploadDefinitionId, params);
        }
        fileUploadStateFuture = fileUploadStateFuture.compose(def ->
          fileService.saveFileChunk(fileId, def, data, params)
            .compose(fileDefinition -> data.length == 0 ?
              fileService.afterFileSave(fileDefinition, params)
              : Future.succeededFuture(def)));
        responseFuture = fileUploadStateFuture.map(PostDataImportUploadDefinitionsFilesByUploadDefinitionIdAndFileIdResponse::respond200WithApplicationJson);
      } else {
        responseFuture = uploadDefinitionService.updateFileDefinitionStatus(uploadDefinitionId, fileId, ERROR, tenantId)
          .map(this::areAllFileDefinitionsFailed)
          .compose(filesFailed -> filesFailed
            ? uploadDefinitionService.updateUploadDefinitionStatus(uploadDefinitionId, UploadDefinition.Status.ERROR, tenantId)
            : Future.succeededFuture())
          .map(String.format("Upload stream for file with id '%s' has been interrupted", fileId))
          .map(PostDataImportUploadDefinitionsFilesByUploadDefinitionIdAndFileIdResponse::respond400WithTextPlain);
      }
      responseFuture.map(Response.class::cast)
        .otherwise(ExceptionHelper::mapExceptionToResponse)
        .onComplete(asyncResultHandler);
    } catch (Exception e) {
      LOGGER.warn("postDataImportUploadDefinitionsFilesByUploadDefinitionIdAndFileId:: Cannot create upload definitions files by uploadDefinitionId {} and fileId {}", uploadDefinitionId, fileId);
      asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
    }
  }

  private boolean areAllFileDefinitionsFailed(UploadDefinition uploadDefinition) {
    return uploadDefinition.getFileDefinitions().stream()
      .allMatch(fileDefinition -> fileDefinition.getStatus().equals(ERROR));
  }

  @Override
  public void postDataImportUploadDefinitionsProcessFilesByUploadDefinitionId(String uploadDefinitionId, ProcessFilesRqDto entity,
                                                                              Map<String, String> okapiHeaders,
                                                                              Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(c -> {
      try {
        LOGGER.info("postDataImportUploadDefinitionsProcessFilesByUploadDefinitionId:: Starting file processing for upload definition {}", uploadDefinitionId);
        fileProcessor.process(JsonObject.mapFrom(entity), JsonObject.mapFrom(okapiHeaders));
        Future.succeededFuture()
          .map(PostDataImportUploadDefinitionsProcessFilesByUploadDefinitionIdResponse.respond204())
          .map(Response.class::cast)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOGGER.warn("postDataImportUploadDefinitionsProcessFilesByUploadDefinitionId:: Cannot upload definitions process files by uploadDefinitionId {}", uploadDefinitionId);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getDataImportFileExtensions(String query, int offset, int limit, String lang, Map<String, String> okapiHeaders,
                                          Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        LOGGER.debug("getDataImportFileExtensions:: query {}", query);
        fileExtensionService.getFileExtensions(query, offset, limit, tenantId)
          .map(GetDataImportFileExtensionsResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOGGER.warn("getDataImportFileExtensions:: Failed to get all file extensions", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void postDataImportFileExtensions(String lang, FileExtension entity, Map<String, String> okapiHeaders,
                                           Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        LOGGER.debug("postDataImportFileExtensions:: fileExtension {}", entity.getExtension());
        validateFileExtension(entity).onComplete(errors -> {
          if (errors.failed()) {
            LOGGER.warn(FILE_EXTENSION_VALIDATE_ERROR_MESSAGE, errors.cause());
            asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(errors.cause())));
          } else if (errors.result().getTotalRecords() > 0) {
            asyncResultHandler.handle(Future.succeededFuture(PostDataImportFileExtensionsResponse.respond422WithApplicationJson(errors.result())));
          } else {
            fileExtensionService.addFileExtension(entity, new OkapiConnectionParams(okapiHeaders, vertxContext.owner()))
              .map((Response) PostDataImportFileExtensionsResponse
                .respond201WithApplicationJson(entity, PostDataImportFileExtensionsResponse.headersFor201()))
              .otherwise(ExceptionHelper::mapExceptionToResponse)
              .onComplete(asyncResultHandler);
          }
        });
      } catch (Exception e) {
        LOGGER.warn("postDataImportFileExtensions:: Failed to create file extension", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getDataImportFileExtensionsById(String id, String lang, Map<String, String> okapiHeaders,
                                              Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(c -> {
      try {
        LOGGER.debug("getDataImportFileExtensionsById:: fileExtensionId {}", id);
        fileExtensionService.getFileExtensionById(id, tenantId)
          .map(optionalFileExtension -> optionalFileExtension.orElseThrow(() ->
            new NotFoundException(String.format("FileExtension with id '%s' was not found", id))))
          .map(GetDataImportFileExtensionsByIdResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOGGER.warn("getDataImportFileExtensionsById:: Failed to get file extension by id", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void putDataImportFileExtensionsById(String id, String lang, FileExtension entity, Map<String, String> okapiHeaders,
                                              Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        LOGGER.debug("putDataImportFileExtensionsById:: fileExtensionId {}", id);
        validateFileExtension(entity).onComplete(errors -> {
          entity.setId(id);
          if (errors.failed()) {
            LOGGER.warn(FILE_EXTENSION_VALIDATE_ERROR_MESSAGE, errors.cause());
            asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(errors.cause())));
          } else if (errors.result().getTotalRecords() > 0) {
            asyncResultHandler.handle(Future.succeededFuture(PutDataImportFileExtensionsByIdResponse.respond422WithApplicationJson(errors.result())));
          } else {
            fileExtensionService.updateFileExtension(entity, new OkapiConnectionParams(okapiHeaders, vertxContext.owner()))
              .map(updatedEntity -> (Response) PutDataImportFileExtensionsByIdResponse.respond200WithApplicationJson(updatedEntity))
              .otherwise(ExceptionHelper::mapExceptionToResponse)
              .onComplete(asyncResultHandler);
          }
        });
      } catch (Exception e) {
        LOGGER.warn("putDataImportFileExtensionsById:: Failed to update file extension", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void deleteDataImportFileExtensionsById(String id, String lang, Map<String, String> okapiHeaders,
                                                 Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        LOGGER.debug("deleteDataImportFileExtensionsById:: fileExtensionId {}", id);
        fileExtensionService.deleteFileExtension(id, tenantId)
          .map(deleted -> deleted ?
            DeleteDataImportFileExtensionsByIdResponse.respond204() :
            DeleteDataImportFileExtensionsByIdResponse.respond404WithTextPlain(
              String.format("FileExtension with id '%s' was not found", id)))
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOGGER.warn("deleteDataImportFileExtensionsById:: Failed to delete file extension", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void postDataImportFileExtensionsRestoreDefault(Map<String, String> okapiHeaders,
                                                         Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        LOGGER.debug("postDataImportFileExtensionsRestoreDefault:: tenantId {}", tenantId);
        fileExtensionService.restoreFileExtensions(tenantId)
          .map(defaultCollection -> (Response) PostDataImportFileExtensionsRestoreDefaultResponse
            .respond200WithApplicationJson(defaultCollection))
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOGGER.warn("postDataImportFileExtensionsRestoreDefault:: Failed to restore file extensions", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getDataImportDataTypes(Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
                                     Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        LOGGER.debug("getDataImportDataTypes:: getting data import types");
        fileExtensionService.getDataTypes()
          .map(GetDataImportDataTypesResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOGGER.warn("getDataImportDataTypes:: Failed to get all data types", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getDataImportUploadUrl(String fileName, Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        LOGGER.debug("getDataImportUploadUrl:: getting upload url for filename {}", fileName);
        minioStorageService.getFileUploadFirstPartUrl(fileName, tenantId)
          .map(GetDataImportUploadUrlResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOGGER.warn("getDataImportUploadUrl:: Failed to get upload url", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }

  @Override
  public void getDataImportUploadUrlSubsequent(String key, String uploadId, int partNumber, Map<String, String> okapiHeaders,
                                     Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        LOGGER.debug(
          "getDataImportUploadUrlSubsequent:: getting subsequent upload url, part #{} of key {} (upload ID {})",
          partNumber,
          key,
          uploadId
        );
        minioStorageService.getFileUploadPartUrl(key, uploadId, partNumber)
          .map(GetDataImportUploadUrlSubsequentResponse::respond200WithApplicationJson)
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOGGER.warn("getDataImportUploadUrl:: Failed to get upload url", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
  }
  
  @Override
  public void getDataImportSplitStatus(Map<String, String> okapiHeaders, Handler<AsyncResult<Response>> asyncResultHandler,
      Context vertxContext) {
    vertxContext.runOnContext(v -> {
      Promise<SplitStatus> splitconfigpromise = Promise.promise();
      SplitStatus response = new SplitStatus();
      response.setSplitStatus(this.splitFileProcess);
      splitconfigpromise.complete(response);
      splitconfigpromise.future()
      .map(GetDataImportSplitStatusResponse::respond200WithApplicationJson)
      .map(Response.class::cast)
      .onComplete(asyncResultHandler); 
    });
    
  }
  @Override
  public void postDataImportAssembleStorageFile(AssembleFileDto entity, Map<String, String> okapiHeaders,
      Handler<AsyncResult<Response>> asyncResultHandler, Context vertxContext) {
    vertxContext.runOnContext(v -> {
      try {
        LOGGER.debug(
          "postDataImportAssembleStorageFile:: Assemble Storage File to complete upload {}",
          entity.getKey()
        );
        minioStorageService.completeMultipartFileUpload(entity.getKey(),  entity.getUploadId(),entity.getTags())
          .map(  completed -> Boolean.TRUE.equals(completed) ? PostDataImportAssembleStorageFileResponse.respond204() : PostDataImportAssembleStorageFileResponse.respond400WithTextPlain("Failed to assemble Data Import upload file") )
          .map(Response.class::cast)
          .otherwise(ExceptionHelper::mapExceptionToResponse)
          .onComplete(asyncResultHandler);
      } catch (Exception e) {
        LOGGER.warn("getDataImportUploadUrl:: Failed to assemble file upload", e);
        asyncResultHandler.handle(Future.succeededFuture(ExceptionHelper.mapExceptionToResponse(e)));
      }
    });
    
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
      .compose(errorsReply -> fileExtensionService.isFileExtensionExistByName(extension, tenantId))
      .map(exist -> exist && errors.getTotalRecords() == 0
        ? errors.withErrors(Collections.singletonList(new Error().withMessage(FILE_EXTENSION_DUPLICATE_ERROR_CODE))).withTotalRecords(errors.getErrors().size() + 1)
        : errors);
  }

  private String addCreatedByConditionToCqlQuery(String cqlQuery, Map<String, String> okapiHeaders) {
    String userId = okapiHeaders.get(OKAPI_USERID_HEADER);
    String token = okapiHeaders.get(RestVerticle.OKAPI_HEADER_TOKEN);
    if (userId == null && token != null) {
      userId = getUserIdFromToken(token);
    }
    if (StringUtils.isNotBlank(cqlQuery)) {
      return String.format("metadata.createdByUserId == %s and %s", userId, cqlQuery);
    }
    return cqlQuery;
  }

  private String getUserIdFromToken(String token) {
    try {
      String[] split = token.split("\\.");
      String json = getJson(split[1]);
      JsonObject tokenJson = new JsonObject(json);
      return tokenJson.getString("user_id");
    } catch (Exception e) {
      LOGGER.warn("getUserIdFromToken:: Invalid x-okapi-token: " + token, e);
      return null;
    }
  }

  private static String getJson(String strEncoded) {
    byte[] decodedBytes = Base64.getDecoder().decode(strEncoded);
    return new String(decodedBytes, StandardCharsets.UTF_8);
  }




}
