package org.folio.service.auth;

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
import org.springframework.stereotype.Component;

@Component
public class AuthClient extends ApiClient {

  private static final Logger LOGGER = LogManager.getLogger();
  private static final String LOGIN_ENDPOINT = "authn/login";
  private static final String CREDENTIALS_ENDPOINT = "authn/credentials";

  public String login(OkapiConnectionParams params, LoginCredentials payload) {
    return sendRequest(
      new HttpPost(),
      params,
      LOGIN_ENDPOINT,
      null,
      payload,
      ApiClient::getResponseEntity
    )
      .orElseThrow()
      .getString("okapiToken");
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

    public String toString() {
      if (this.getPassword().isBlank()) {
        return (
          "LoginCredentials(username=" +
          this.getUsername() +
          ", password=<not set>, tenant=" +
          this.getTenant() +
          ")"
        );
      } else {
        return (
          "LoginCredentials(username=" +
          this.getUsername() +
          ", password=<set>, tenant=" +
          this.getTenant() +
          ")"
        );
      }
    }
  }
}
