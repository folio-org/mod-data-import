package org.folio.service.file;

import static org.assertj.core.api.Assertions.assertThat;

import io.vertx.core.impl.VertxImpl;
import io.vertx.core.internal.deployment.DeploymentManager;
import org.folio.support.AbstractRestTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class S3JobRunningVerticleDisabledIntegrationTest extends AbstractRestTest {

  static {
    System.setProperty("SPLIT_FILES_ENABLED", "false");
  }

  @AfterAll
  static void resetEnv() {
    System.clearProperty("SPLIT_FILES_ENABLED");
  }

  @Override
  protected boolean shareVerticle() {
    // SPLIT_FILES_ENABLED is read once, when the verticle first boots; this class must not
    // reuse a shared deployment that another test class already booted with splitting enabled.
    return false;
  }

  @Override
  protected String getModuleName() {
    // A distinct key guarantees a genuinely fresh deployment instead of one inherited from
    // an ancestor JUnit store (see S3JobRunningVerticleEnabledIntegrationTest).
    return super.getModuleName() + "-split-files-disabled";
  }

  @DisplayName("should not deploy S3JobRunningVerticle when splitting is disabled")
  @Test
  void shouldNotDeployS3JobRunningVerticle_whenSplittingIsDisabled() {
    DeploymentManager deploymentManager = ((VertxImpl) vertx).deploymentManager();

    assertThat(vertx.deploymentIDs().stream()
      .map(deploymentManager::deployment)
      .map(d -> d.deployment().identifier())
      .toList())
      .doesNotContain("java:org.folio.service.file.S3JobRunningVerticle");
  }
}
