package org.folio.service.cleanup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.folio.rest.jaxrs.model.UploadDefinition.Status.COMPLETED;
import static org.folio.rest.jaxrs.model.UploadDefinition.Status.LOADED;
import static org.folio.support.TestUtil.TENANT_ID;

import io.vertx.junit5.VertxTestContext;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Date;
import java.util.Map;
import org.apache.commons.collections4.map.HashedMap;
import org.apache.commons.io.FileUtils;
import org.folio.dao.UploadDefinitionDao;
import org.folio.dataimport.util.ConnectionParams;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.jaxrs.model.FileDefinition;
import org.folio.rest.jaxrs.model.Metadata;
import org.folio.rest.jaxrs.model.UploadDefinition;
import org.folio.support.AbstractRestTest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

class StorageCleanupServiceImplTest extends AbstractRestTest {

  private static final String STORAGE_PATH = "./storage";
  private static final long ONE_HOUR_MILLIS = 3600000;

  private static final File testFile = new File(STORAGE_PATH + "/marc.mrc");

  @Autowired
  private UploadDefinitionDao uploadDefinitionDao;
  @Autowired
  private StorageCleanupService storageCleanupService;

  private ConnectionParams connectionParams;
  private UploadDefinition uploadDefinition;

  @BeforeEach
  void setUpStorage() throws IOException {
    Map<String, String> headers = new HashedMap<>();
    headers.put(XOkapiHeaders.TENANT, TENANT_ID);
    headers.put(XOkapiHeaders.URL, mockServerUrl());
    connectionParams = new ConnectionParams(headers);

    var fileDefinition = new FileDefinition()
      .withId("776c7413-7ad9-467b-a686-775a434d2505")
      .withSourcePath(testFile.getPath())
      .withUiKey("marc.mrc.md1547160916680")
      .withName("marc.mrc")
      .withStatus(FileDefinition.Status.UPLOADED)
      .withUploadDefinitionId("71a43ec9-d923-4c44-8405-979af23b7cc9")
      .withSize(209);

    uploadDefinition = new UploadDefinition()
      .withId("71a43ec9-d923-4c44-8405-979af23b7cc9")
      .withMetaJobExecutionId("4044bf4d-fb53-4b01-81e9-fafff1024dde")
      .withStatus(LOADED)
      .withFileDefinitions(Collections.singletonList(fileDefinition))
      .withMetadata(new Metadata().withCreatedDate(new Date()).withUpdatedDate(new Date()));

    Files.createDirectories(Paths.get(testFile.getParent()));
    Files.createFile(Paths.get(testFile.getPath()));
  }

  @AfterEach
  void tearDownFileSystem() throws IOException {
    FileUtils.deleteDirectory(new File(STORAGE_PATH));
  }

  @DisplayName("should remove file and return true when upload definition has COMPLETED status")
  @Test
  void shouldRemoveFileAndReturnTrue_whenUploadDefinitionIsCompleted(VertxTestContext testContext) {
    uploadDefinition.setStatus(COMPLETED);
    uploadDefinitionDao.addUploadDefinition(uploadDefinition, TENANT_ID)
      .compose(v -> storageCleanupService.cleanStorage(connectionParams))
      .onComplete(testContext.succeeding(result -> testContext.verify(() -> {
        assertThat(result).isTrue();
        assertThat(testFile).doesNotExist();
        testContext.completeNow();
      })));
  }

  @DisplayName("should remove file and return true when upload definition was updated over one hour ago")
  @Test
  void shouldRemoveFileAndReturnTrue_whenUploadDefinitionIsOlderThanOneHour(VertxTestContext testContext) {
    uploadDefinition.getMetadata()
      .withCreatedDate(new Date(new Date().getTime() - ONE_HOUR_MILLIS))
      .withUpdatedDate(new Date(new Date().getTime() - ONE_HOUR_MILLIS));

    uploadDefinitionDao.addUploadDefinition(uploadDefinition, TENANT_ID)
      .compose(v -> storageCleanupService.cleanStorage(connectionParams))
      .onComplete(testContext.succeeding(result -> testContext.verify(() -> {
        assertThat(result).isTrue();
        assertThat(testFile).doesNotExist();
        testContext.completeNow();
      })));
  }

  @DisplayName("should return false and keep file when upload definition is recent and not completed")
  @Test
  void shouldReturnFalseAndKeepFile_whenUploadDefinitionIsRecentAndNotCompleted(VertxTestContext testContext) {
    uploadDefinitionDao.addUploadDefinition(uploadDefinition, TENANT_ID)
      .compose(v -> storageCleanupService.cleanStorage(connectionParams))
      .onComplete(testContext.succeeding(result -> testContext.verify(() -> {
        assertThat(result).isFalse();
        assertThat(testFile).exists();
        testContext.completeNow();
      })));
  }

  @DisplayName("should return false when file linked to completed upload definition does not exist")
  @Test
  void shouldReturnFalse_whenFileLinkedToCompletedUploadDefinitionDoesNotExist(VertxTestContext testContext) {
    assertThat(testFile.delete()).isTrue();
    uploadDefinition.setStatus(COMPLETED);

    uploadDefinitionDao.addUploadDefinition(uploadDefinition, TENANT_ID)
      .compose(v -> storageCleanupService.cleanStorage(connectionParams))
      .onComplete(testContext.succeeding(result -> testContext.verify(() -> {
        assertThat(result).isFalse();
        testContext.completeNow();
      })));
  }
}
