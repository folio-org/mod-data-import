package org.folio.service.auth;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpPost;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.okapi.common.WebClientFactory;
import org.folio.okapi.common.refreshtoken.client.ClientOptions;
import org.folio.okapi.common.refreshtoken.client.impl.LoginClient;
import org.folio.okapi.common.refreshtoken.tokencache.TenantUserCache;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class AuthClient extends ApiClient {

  private static final Logger LOGGER = LogManager.getLogger();
  private static final String CREDENTIALS_ENDPOINT = "authn/credentials";

  private static final int CACHE_SIZE = 5;

  private TenantUserCache cache;
  private Vertx vertx;

  @Autowired
  public AuthClient(Vertx vertx) {
    this.vertx = vertx;

    this.cache = new TenantUserCache(CACHE_SIZE);
  }

  public Future<String> login(
    OkapiConnectionParams params,
    LoginCredentials payload
  ) {
    // use standardized RTR token utility
    return new LoginClient(
      new ClientOptions()
        .okapiUrl(params.getOkapiUrl())
        .webClient(WebClientFactory.getWebClient(vertx)),
      cache,
      payload.getTenant(),
      payload.getUsername(),
      () -> Future.succeededFuture(payload.getPassword())
    )
      .getToken();
  }

  public void saveCredentials(
    OkapiConnectionParams params,
    LoginCredentials payload
  ) {
    if (
      Boolean.FALSE.equals(
        sendRequest(
          new HttpPost(),
          params,
          CREDENTIALS_ENDPOINT,
          null,
          payload,
          ApiClient::isResponseOk
        )
      )
    ) {
      throw new IllegalStateException("Unable to save credentials");
    }
  }

  public void deleteCredentials(OkapiConnectionParams params, String userId) {
    if (
      Boolean.TRUE.equals(
        sendRequest(
          new HttpDelete(),
          params,
          CREDENTIALS_ENDPOINT,
          Map.of("userId", userId),
          ApiClient::isResponseOk
        )
      )
    ) {
      LOGGER.info("Deleted existing credentials for system user");
    } else {
      LOGGER.warn("Unable to delete existing credentials for system user");
    }
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class LoginCredentials {

    private String username;
    private String password;
    private String tenant;
  }
}
