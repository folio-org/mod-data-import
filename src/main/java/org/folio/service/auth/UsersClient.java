package org.folio.service.auth;

import io.vertx.core.json.JsonObject;
import java.util.Map;
import java.util.Optional;
import lombok.Builder;
import lombok.Data;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.springframework.stereotype.Component;

@Component
public class UsersClient extends ApiClient {

  private static final String BASE_ENDPOINT = "users";
  private static final String GET_USERS_BY_USERNAME_QUERY = "username=\"%s\"";

  public Optional<User> getUserByUsername(
    OkapiConnectionParams params,
    String username
  ) {
    return get(
      params,
      BASE_ENDPOINT,
      Map.of("query", String.format(GET_USERS_BY_USERNAME_QUERY, username))
    )
      .orElseThrow()
      .getJsonArray("users")
      .stream()
      .findFirst()
      .map(o -> (JsonObject) o)
      .map(o -> o.mapTo(User.class));
  }

  public User createUser(OkapiConnectionParams params, User user) {
    return post(params, BASE_ENDPOINT, user).orElseThrow().mapTo(User.class);
  }

  @Data
  @Builder
  public static class User {

    private String id;
    private String username;
    private boolean active;
    private Personal personal;

    @Data
    @Builder
    public static class Personal {

      private String lastName;
    }
  }
}
