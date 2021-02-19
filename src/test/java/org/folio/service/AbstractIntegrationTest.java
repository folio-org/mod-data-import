package org.folio.service;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;

import io.vertx.core.Context;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;

import org.folio.rest.RestVerticle;
import org.folio.rest.client.TenantClient;
import org.folio.rest.jaxrs.model.TenantAttributes;
import org.folio.rest.jaxrs.model.TenantJob;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.tools.PomReader;
import org.folio.rest.tools.utils.NetworkUtils;
import org.folio.service.config.ApplicationTestConfig;
import org.folio.spring.SpringContextUtil;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.net.URLEncoder;

@RunWith(VertxUnitRunner.class)
public abstract class AbstractIntegrationTest {

  protected static final String HTTP_PORT = "http.port";
  protected static final String TOKEN = "token";
  protected static final String TENANT_ID = "diku";

  protected static Vertx vertx = Vertx.vertx();

  @Rule
  public WireMockRule mockServer = new WireMockRule(
    WireMockConfiguration.wireMockConfig()
      .dynamicPort()
      .notifier(new Slf4jNotifier(true)));

  public AbstractIntegrationTest() {
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
        TenantAttributes tenantAttributes = new TenantAttributes();
        tenantAttributes.setModuleTo(PomReader.INSTANCE.getModuleName());
        tenantClient.postTenant(tenantAttributes, res2 -> {
          async.complete();
        });
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
  }

  protected abstract void clearTable(TestContext context);

  @AfterClass
  public static void tearDownClass(final TestContext context) {
    Async async = context.async();
    vertx.close(context.asyncAssertSuccess(res -> {
      PostgresClient.stopEmbeddedPostgres();
      async.complete();
    }));
  }
}
