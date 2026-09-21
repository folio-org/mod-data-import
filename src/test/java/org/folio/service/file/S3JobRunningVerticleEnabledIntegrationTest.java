package org.folio.service.file;

import static org.assertj.core.api.Assertions.assertThat;

import io.vertx.core.impl.VertxImpl;
import io.vertx.core.internal.deployment.DeploymentManager;
import org.folio.support.AbstractRestTest;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class S3JobRunningVerticleEnabledIntegrationTest extends AbstractRestTest {

  static {
    System.setProperty("SPLIT_FILES_ENABLED", "true");
    System.setProperty("SYSTEM_PROCESSING_PASSWORD", "password");
  }

  @AfterAll
  static void resetEnv() {
    System.clearProperty("SPLIT_FILES_ENABLED");
    System.clearProperty("SYSTEM_PROCESSING_PASSWORD");
  }

  @Override
  protected boolean shareVerticle() {
    // SPLIT_FILES_ENABLED is read once, when the verticle first boots; this class must not
    // share a deployment that another test class may have already booted with the flag unset.
    return false;
  }

  @Override
  protected String getModuleName() {
    // JUnit's per-class store still inherits values from ancestor (e.g. root) stores, so reusing
    // the standard module id here would still find and reuse another class's shared deployment
    // even with shareVerticle() == false. A distinct key guarantees a genuinely fresh deployment.
    return super.getModuleName() + "-split-files-enabled";
  }

  @DisplayName("should deploy S3JobRunningVerticle when splitting is enabled")
  @Test
  void shouldDeployS3JobRunningVerticle_whenSplittingIsEnabled() {
    DeploymentManager deploymentManager = ((VertxImpl) vertx).deploymentManager();

    assertThat(
      vertx.deploymentIDs().stream()
        .map(deploymentManager::deployment)
        .map(d -> d.deployment().identifier())
        .toList())
      .contains("java:org.folio.service.file.S3JobRunningVerticle");
  }
}
