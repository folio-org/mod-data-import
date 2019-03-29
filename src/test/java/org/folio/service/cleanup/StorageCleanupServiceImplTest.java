package org.folio.service.cleanup;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import io.vertx.core.Context;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.apache.commons.collections4.map.HashedMap;
import org.apache.commons.io.FileUtils;
import org.folio.dao.UploadDefinitionDao;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.RestVerticle;
import org.folio.rest.client.TenantClient;
import org.folio.rest.jaxrs.model.FileDefinition;
import org.folio.rest.jaxrs.model.Metadata;
import org.folio.rest.jaxrs.model.UploadDefinition;
import org.folio.rest.persist.Criteria.Criterion;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.utils.NetworkUtils;
import org.folio.service.cleanup.config.ApplicationTestConfig;
import org.folio.spring.SpringContextUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.io.File;
import java.io.IOException;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Date;
import java.util.Map;

import static org.folio.dataimport.util.RestUtil.OKAPI_TENANT_HEADER;
import static org.folio.dataimport.util.RestUtil.OKAPI_URL_HEADER;
import static org.folio.rest.jaxrs.model.UploadDefinition.Status.COMPLETED;
import static org.folio.rest.jaxrs.model.UploadDefinition.Status.LOADED;

@RunWith(VertxUnitRunner.class)
public class StorageCleanupServiceImplTest {

  private static final String HTTP_PORT = "http.port";
  private static final String TOKEN = "token";
  private static final String UPLOAD_DEFINITIONS_TABLE = "upload_definitions";
  private static final String TENANT_ID = "diku";
  private static final String STORAGE_PATH = "./storage";
  private static final long ONE_HOUR_MILLIS = 3600000;

  private static Vertx vertx = Vertx.vertx();
  private static File testFile = new File(STORAGE_PATH + "/" + "marc.mrc");

  @Autowired
  private  UploadDefinitionDao uploadDefinitionDao;
  @Autowired
  private StorageCleanupService storageCleanupService;
  private OkapiConnectionParams okapiParams;
  private UploadDefinition uploadDefinition;

  @Rule
  public WireMockRule mockServer = new WireMockRule(
    WireMockConfiguration.wireMockConfig()
      .dynamicPort()
      .notifier(new Slf4jNotifier(true)));

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

  @BeforeClass
  public static void setUpClass(TestContext context) throws Exception {
    Async async = context.async();
    int port = NetworkUtils.nextFreePort();
    String okapiUrl = "http://localhost:" + port;
    PostgresClient.stopEmbeddedPostgres();
    PostgresClient.closeAllClients();
    PostgresClient.setIsEmbedded(true);
    PostgresClient.getInstance(vertx).startEmbeddedPostgres();
    TenantClient tenantClient = new TenantClient(okapiUrl, TENANT_ID, TOKEN);

    final DeploymentOptions options = new DeploymentOptions().setConfig(new JsonObject().put(HTTP_PORT, port));
    vertx.deployVerticle(RestVerticle.class.getName(), options, res -> {
      try {
        tenantClient.postTenant(null, res2 -> async.complete());
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  @Before
  public void setUp(TestContext context) throws IOException {
    clearTable(context);
    WireMock.stubFor(WireMock.get("/configurations/entries?query="
      + URLEncoder.encode("module==DATA_IMPORT AND ( code==\"data.import.storage.type\")", "UTF-8")
      + "&offset=0&limit=3&")
      .willReturn(WireMock.serverError()));
    WireMock.stubFor(WireMock.get("/configurations/entries?query="
      + URLEncoder.encode("module==DATA_IMPORT AND ( code==\"data.import.cleanup.time\")", "UTF-8")
      + "&offset=0&limit=3&")
      .willReturn(WireMock.serverError()));

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

  private void clearTable(TestContext context) {
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

  @AfterClass
  public static void tearDownClass(final TestContext context) {
    Async async = context.async();
    vertx.close(context.asyncAssertSuccess(res -> {
      PostgresClient.stopEmbeddedPostgres();
      async.complete();
    }));
  }

  @Test
  public void shouldRemoveFileAndReturnSucceededFutureWithTrueWhenUploadDefinitionHasStatusCompleted(TestContext context) {
    Async async = context.async();
    uploadDefinition.setStatus(COMPLETED);
    uploadDefinitionDao.addUploadDefinition(uploadDefinition, TENANT_ID).compose(saveAr -> {

      Future<Boolean> future = storageCleanupService.cleanStorage(okapiParams);

      future.setHandler(ar -> {
        Assert.assertTrue(ar.succeeded());
        Assert.assertTrue(ar.result());
        Assert.assertFalse(testFile.exists());
        async.complete();
      });
      return Future.future();
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

      future.setHandler(ar -> {
        Assert.assertTrue(ar.succeeded());
        Assert.assertTrue(ar.result());
        Assert.assertFalse(testFile.exists());
        async.complete();
      });
      return Future.future();
    });
  }

  @Test
  public void shouldReturnSucceededFutureWithFalseWhenUploadDefinitionNotCompletedAndHasNotBeenUpdatedOneHourAgoAndShouldNotRemoveFile(TestContext context) {
    Async async = context.async();
    uploadDefinitionDao.addUploadDefinition(uploadDefinition, TENANT_ID).compose(saveAr -> {

      Future<Boolean> future = storageCleanupService.cleanStorage(okapiParams);

      future.setHandler(ar -> {
        Assert.assertTrue(ar.succeeded());
        Assert.assertFalse(ar.result());
        Assert.assertTrue(testFile.exists());
        async.complete();
      });
      return Future.future();
    });
  }

  @Test
  public void shouldReturnSucceededFutureWithFalseWhenFileLinkedToCompletedUploadDefinitionDoesNotExist(TestContext context) {
    context.assertTrue(testFile.delete());
    Async async = context.async();
    uploadDefinition.setStatus(COMPLETED);
    uploadDefinitionDao.addUploadDefinition(uploadDefinition, TENANT_ID).compose(saveAr -> {

      Future<Boolean> future = storageCleanupService.cleanStorage(okapiParams);

      future.setHandler(ar -> {
        context.assertTrue(ar.succeeded());
        Assert.assertFalse(ar.result());
        async.complete();
      });
      return Future.future();
    });
  }

}
