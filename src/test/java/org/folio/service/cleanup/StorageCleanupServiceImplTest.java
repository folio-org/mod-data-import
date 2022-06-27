package org.folio.service.cleanup;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import org.apache.commons.collections4.map.HashedMap;
import org.apache.commons.io.FileUtils;
import org.folio.dao.UploadDefinitionDao;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.FileDefinition;
import org.folio.rest.jaxrs.model.Metadata;
import org.folio.rest.jaxrs.model.UploadDefinition;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.folio.service.AbstractIntegrationTest;
import org.folio.service.config.ApplicationTestConfig;
import org.folio.spring.SpringContextUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Date;
import java.util.Map;

import static org.folio.dataimport.util.RestUtil.OKAPI_TENANT_HEADER;
import static org.folio.dataimport.util.RestUtil.OKAPI_URL_HEADER;
import static org.folio.rest.jaxrs.model.UploadDefinition.Status.COMPLETED;
import static org.folio.rest.jaxrs.model.UploadDefinition.Status.LOADED;

public class StorageCleanupServiceImplTest extends AbstractIntegrationTest {

  private static final String UPLOAD_DEFINITIONS_TABLE = "upload_definitions";
  private static final String STORAGE_PATH = "./storage";
  private static final long ONE_HOUR_MILLIS = 3600000;

  private static Vertx vertx = Vertx.vertx();
  private static File testFile = new File(STORAGE_PATH + "/" + "marc.mrc");

  @Autowired
  private UploadDefinitionDao uploadDefinitionDao;
  @Autowired
  private StorageCleanupService storageCleanupService;
  private OkapiConnectionParams okapiParams;
  private UploadDefinition uploadDefinition;

  private FileDefinition fileDefinition = new FileDefinition()
    .withId("776c7413-7ad9-467b-a686-775a434d2505")
    .withSourcePath(testFile.getPath())
    .withUiKey("marc.mrc.md1547160916680")
    .withName("marc.mrc")
    .withStatus(FileDefinition.Status.UPLOADED)
    .withUploadDefinitionId("71a43ec9-d923-4c44-8405-979af23b7cc9")
    .withSize(209);

  public StorageCleanupServiceImplTest() {
    Context vertxContext = vertx.getOrCreateContext();
    SpringContextUtil.init(vertx, vertxContext, ApplicationTestConfig.class);
    SpringContextUtil.autowireDependencies(this, vertxContext);
  }

  @Before
  public void setUp(TestContext context) throws IOException {
    super.setUp(context);

    Map<String, String> headers = new HashedMap<>();
    headers.put(OKAPI_TENANT_HEADER, TENANT_ID);
    headers.put(OKAPI_URL_HEADER, "http://localhost:" + mockServer.port());
    okapiParams = new OkapiConnectionParams(headers, vertx);


    uploadDefinition = new UploadDefinition()
      .withId("71a43ec9-d923-4c44-8405-979af23b7cc9")
      .withMetaJobExecutionId("4044bf4d-fb53-4b01-81e9-fafff1024dde")
      .withStatus(LOADED)
      .withFileDefinitions(Collections.singletonList(fileDefinition))
      .withMetadata(new Metadata()
        .withCreatedDate(new Date())
        .withUpdatedDate(new Date()));

    Files.createDirectory(Paths.get(testFile.getParent()));
    Files.createFile(Paths.get(testFile.getPath()));
  }

  @Override
  protected void clearTable(TestContext context) {
    Async async = context.async();
    PostgresClient.getInstance(vertx, TENANT_ID).delete(UPLOAD_DEFINITIONS_TABLE, new Criterion(), event -> {
      if (event.failed()) {
        context.fail(event.cause());
      }
      async.complete();
    });
  }

  @After
  public void tearDownFileSystem() throws IOException {
    FileUtils.deleteDirectory(new File(STORAGE_PATH));
  }

  @Test
  public void shouldRemoveFileAndReturnSucceededFutureWithTrueWhenUploadDefinitionHasStatusCompleted(TestContext context) {
    Async async = context.async();
    uploadDefinition.setStatus(COMPLETED);
    uploadDefinitionDao.addUploadDefinition(uploadDefinition, TENANT_ID).compose(saveAr -> {

      Future<Boolean> future = storageCleanupService.cleanStorage(okapiParams);

      future.onComplete(ar -> {
        context.assertTrue(ar.succeeded());
        context.assertTrue(ar.result());
        context.assertFalse(testFile.exists());
        async.complete();
      });
      return future;
    });
  }

  @Test
  public void shouldRemoveFileAndReturnSucceededFutureWhenUploadDefinitionHasBeenUpdatedOneHourAgo(TestContext context) {
    Async async = context.async();
    uploadDefinition.getMetadata()
      .withCreatedDate(new Date(new Date().getTime() - ONE_HOUR_MILLIS))
      .withUpdatedDate(new Date(new Date().getTime() - ONE_HOUR_MILLIS));
    uploadDefinitionDao.addUploadDefinition(uploadDefinition, TENANT_ID).compose(saveAr -> {

      Future<Boolean> future = storageCleanupService.cleanStorage(okapiParams);

      future.onComplete(ar -> {
        context.assertTrue(ar.succeeded());
        context.assertTrue(ar.result());
        context.assertFalse(testFile.exists());
        async.complete();
      });
      return future;
    });
  }

  @Test
  public void shouldReturnSucceededFutureWithFalseWhenUploadDefinitionNotCompletedAndHasNotBeenUpdatedOneHourAgoAndShouldNotRemoveFile(TestContext context) {
    Async async = context.async();
    uploadDefinitionDao.addUploadDefinition(uploadDefinition, TENANT_ID).compose(saveAr -> {

      Future<Boolean> future = storageCleanupService.cleanStorage(okapiParams);

      future.onComplete(ar -> {
        context.assertTrue(ar.succeeded());
        context.assertFalse(ar.result());
        context.assertTrue(testFile.exists());
        async.complete();
      });
      return future;
    });
  }

  @Test
  public void shouldReturnSucceededFutureWithFalseWhenFileLinkedToCompletedUploadDefinitionDoesNotExist(TestContext context) {
    context.assertTrue(testFile.delete());
    Async async = context.async();
    uploadDefinition.setStatus(COMPLETED);
    uploadDefinitionDao.addUploadDefinition(uploadDefinition, TENANT_ID).compose(saveAr -> {

      Future<Boolean> future = storageCleanupService.cleanStorage(okapiParams);

      future.onComplete(ar -> {
        context.assertTrue(ar.succeeded());
        context.assertFalse(ar.result());
        async.complete();
      });
      return future;
    });
  }

}
