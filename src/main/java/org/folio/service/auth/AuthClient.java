package org.folio.service.auth;

import io.vertx.core.json.JsonObject;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.http.client.methods.HttpPost;
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
