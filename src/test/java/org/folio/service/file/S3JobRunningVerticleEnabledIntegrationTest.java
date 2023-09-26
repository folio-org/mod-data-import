package org.folio.service.file;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;

import io.vertx.core.impl.VertxInternal;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.util.stream.Collectors;
import org.folio.rest.AbstractRestTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class S3JobRunningVerticleEnabledIntegrationTest
  extends AbstractRestTest {

  @BeforeClass
  public static void setUpClass(TestContext context) throws Exception {
    System.setProperty("SPLIT_FILES_ENABLED", "true");

    AbstractRestTest.setUpClass(context);
  }

  @AfterClass
  public static void resetEnv() {
    System.clearProperty("SPLIT_FILES_ENABLED");
  }

  @Test
  public void testRunning() {
    assertThat(
      "S3JobRunningVerticle is deployed when splitting is enabled",
      vertx
        .deploymentIDs()
        .stream()
        .map(id -> ((VertxInternal) vertx).getDeployment(id))
        .map(deployment -> deployment.verticleIdentifier())
        .collect(Collectors.toList()),
      hasItem("java:org.folio.service.file.S3JobRunningVerticle")
    );
  }
}
