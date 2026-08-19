package org.folio.rest;

import org.folio.support.AbstractRestTest;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class OkapiHealthAPITest extends AbstractRestTest {

  private static final String HEALTH_URL = "/admin/health";

  @DisplayName("should return 200 when module is healthy")
  @Test
  void shouldReturn200_whenModuleIsHealthy() {
    getRequest(HEALTH_URL).statusCode(200);
  }
}
