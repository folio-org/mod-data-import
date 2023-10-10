package org.folio.service.auth;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.vertx.core.json.JsonObject;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.springframework.stereotype.Component;

@Component
public class UsersClient extends ApiClient {

  private static final String BASE_ENDPOINT = "users";
  private static final String ARRAY_KEY = "users";
  private static final String GET_USERS_BY_USERNAME_QUERY = "username=\"%s\"";

  public Optional<User> getUserByUsername(
    OkapiConnectionParams params,
    String username
  ) {
    return sendRequest(
      new HttpGet(),
      params,
      BASE_ENDPOINT,
      Map.of("query", String.format(GET_USERS_BY_USERNAME_QUERY, username)),
      ApiClient::getResponseEntity
    )
      .orElseThrow()
      .getJsonArray(ARRAY_KEY)
      .stream()
      .findFirst()
      .map(o -> (JsonObject) o)
      .map(o -> o.mapTo(User.class));
  }

  public User createUser(OkapiConnectionParams params, User user) {
    return sendRequest(
      new HttpPost(),
      params,
      BASE_ENDPOINT,
      null,
      user,
      ApiClient::getResponseEntity
    )
      .orElseThrow()
      .mapTo(User.class);
  }

  public void updateUser(OkapiConnectionParams params, User user) {
    if (
      !sendRequest(
        new HttpPut(),
        params,
        BASE_ENDPOINT + "/" + user.getId(),
        null,
        user,
        ApiClient::isResponseOk
      )
    ) {
      throw new IllegalStateException("Could not update user");
    }
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class User {

    private String id;
    private String username;
    private String type;
    private boolean active;
    private Personal personal;

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Personal {

      private String lastName;
    }
  }
}
