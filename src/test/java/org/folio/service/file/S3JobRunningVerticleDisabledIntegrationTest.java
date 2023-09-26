package org.folio.service.file;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

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
public class S3JobRunningVerticleDisabledIntegrationTest
  extends AbstractRestTest {

  @BeforeClass
  public static void setUpClass(TestContext context) throws Exception {
    System.setProperty("SPLIT_FILES_ENABLED", "false");

    AbstractRestTest.setUpClass(context);
  }

  @AfterClass
  public static void resetEnv() {
    System.clearProperty("SPLIT_FILES_ENABLED");
  }

  @Test
  public void testRunning() {
    assertThat(
      "S3JobRunningVerticle is not deployed when splitting is disabled",
      vertx
        .deploymentIDs()
        .stream()
        .map(id -> ((VertxInternal) vertx).getDeployment(id))
        .map(deployment -> deployment.verticleIdentifier())
        .collect(Collectors.toList()),
      not(hasItem("java:org.folio.service.file.S3JobRunningVerticle"))
    );
  }
}
