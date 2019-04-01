package org.folio.service.cleanup;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.folio.dao.UploadDefinitionDao;
import org.folio.dataimport.util.ConfigurationUtil;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.DefinitionCollection;
import org.folio.rest.jaxrs.model.UploadDefinition;
import org.folio.service.storage.FileStorageService;
import org.folio.service.storage.FileStorageServiceBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.folio.rest.RestVerticle.MODULE_SPECIFIC_ARGS;
import static org.folio.rest.jaxrs.model.UploadDefinition.Status.COMPLETED;

@Service
public class StorageCleanupServiceImpl implements StorageCleanupService {

  private static final String TIME_WITHOUT_UPLOAD_DEFINITION_CHANGES_CODE = "data.import.cleanup.time";
  private static final long TIME_WITHOUT_CHANGES_DEFAULT_VALUE_MILLIS =
    Long.parseLong(MODULE_SPECIFIC_ARGS.getOrDefault(TIME_WITHOUT_UPLOAD_DEFINITION_CHANGES_CODE, "3600000"));
  private static final Logger LOGGER = LoggerFactory.getLogger(StorageCleanupServiceImpl.class);

  @Autowired
  private Vertx vertx;
  @Autowired
  private UploadDefinitionDao uploadDefinitionDao;

  @Override
  public Future<Boolean> cleanStorage(OkapiConnectionParams params) {
    Future<Boolean> future = Future.future();

    return FileStorageServiceBuilder.build(vertx, params.getTenantId(), params)
      .compose(fileStorageService -> getTimeWithoutUploadDefinitionChanges(params)
        .compose(timeWithoutChanges -> {
          Date lastChangesDate = new Date(new Date().getTime() - timeWithoutChanges);
          return uploadDefinitionDao.getUploadDefinitionsByStatusOrUpdatedDateNotGreaterThen(COMPLETED, lastChangesDate, 0, 0, params.getTenantId());
        })
        .map(DefinitionCollection::getUploadDefinitions)
        .compose(uploadDefinitions -> deleteFilesByUploadDefinitions(fileStorageService, uploadDefinitions))
        .compose(compositeFuture -> {
          boolean isFilesDeleted = compositeFuture.<Boolean>list()
            .stream()
            .reduce((a, b) -> a && b)
            .orElse(false);
          if (isFilesDeleted) {
            LOGGER.info("File storage cleaning has been successfully completed");
          } else {
            LOGGER.info("Files have not been removed because files which satisfy search condition does not exist");
          }
          future.complete(isFilesDeleted);
          return future;
        })
      );
  }

  private Future<Long> getTimeWithoutUploadDefinitionChanges(OkapiConnectionParams params) {
    return ConfigurationUtil.getPropertyByCode(TIME_WITHOUT_UPLOAD_DEFINITION_CHANGES_CODE, params)
      .map(Long::parseLong)
      .otherwise(TIME_WITHOUT_CHANGES_DEFAULT_VALUE_MILLIS);
  }

  private Future<CompositeFuture> deleteFilesByUploadDefinitions(FileStorageService fileStorageService, List<UploadDefinition> uploadDefinitions) {
    List<Future> deleteFilesFutures = new ArrayList<>();
    uploadDefinitions.stream()
      .flatMap(uploadDefinition -> uploadDefinition.getFileDefinitions().stream())
      .forEach(fileDefinition -> deleteFilesFutures.add(fileStorageService.deleteFile(fileDefinition)));
    return CompositeFuture.all(deleteFilesFutures);
  }

}
