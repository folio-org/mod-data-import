package org.folio.service.auth;

import io.vertx.core.json.JsonObject;
import java.util.Map;
import java.util.Optional;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.User;
import org.springframework.stereotype.Component;

@Component
public class UsersClient extends ApiClient {

  private static final String BASE_ENDPOINT = "users";
  private static final String GET_USERS_BY_USERNAME_QUERY = "username=\"%s\"";

  public Optional<JsonObject> getUserByUsername(
    OkapiConnectionParams params,
    String username
  ) {
    return get(
      params,
      BASE_ENDPOINT,
      Map.of("query", String.format(GET_USERS_BY_USERNAME_QUERY, username))
    );
  }

  public Optional<JsonObject> createUser(
    OkapiConnectionParams params,
    User user
  ) {
    return post(params, BASE_ENDPOINT, user);
  }
}
