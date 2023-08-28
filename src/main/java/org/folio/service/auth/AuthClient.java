package org.folio.service.auth;

import io.vertx.core.json.JsonObject;
import lombok.Builder;
import lombok.Data;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.springframework.stereotype.Component;

@Component
public class AuthClient extends ApiClient {

  private static final String LOGIN_ENDPOINT = "authn/login";
  private static final String CREDENTIALS_ENDPOINT = "authn/credentials";

  public String login(OkapiConnectionParams params, LoginCredentials payload) {
    return post(params, LOGIN_ENDPOINT, payload)
      .orElseThrow()
      .getString("okapiToken");
  }

  public JsonObject saveCredentials(
    OkapiConnectionParams params,
    LoginCredentials payload
  ) {
    return post(params, CREDENTIALS_ENDPOINT, payload).orElseThrow();
  }

  @Data
  @Builder
  public static class LoginCredentials {

    private String userId;
    private String username;
    private String password;
    private String tenant;
  }
}
