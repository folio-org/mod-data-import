package org.folio.service.auth;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.http.client.methods.HttpPost;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.okapi.common.WebClientFactory;
import org.folio.okapi.common.refreshtoken.client.ClientOptions;
import org.folio.okapi.common.refreshtoken.client.impl.LoginClient;
import org.folio.okapi.common.refreshtoken.tokencache.TenantUserCache;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class AuthClient extends ApiClient {

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
      postOrPut(
        HttpPost::new,
        params,
        CREDENTIALS_ENDPOINT,
        payload,
        r -> {
          // return optional so it can be caught and handled after
          if (r.getStatusLine().getStatusCode() == 201) {
            return Optional.of(new JsonObject());
          } else {
            return Optional.empty();
          }
        }
      )
        .isEmpty()
    ) {
      throw new IllegalStateException();
    }
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  public static class LoginCredentials {

    private String userId;
    private String username;
    private String password;
    private String tenant;
  }
}
