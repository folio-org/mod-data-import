package org.folio.service.auth;

import io.vertx.core.json.JsonObject;
import java.util.Map;
import java.util.Optional;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.PermissionUser;
import org.springframework.stereotype.Component;

@Component
public class PermissionsClient extends ApiClient {

  private static final String BASE_ENDPOINT = "perms/users";
  private static final String BASE_ENDPOINT_WITH_ID = "perms/users/%s";

  public Optional<JsonObject> getPermissionsUserByUserId(
    OkapiConnectionParams params,
    String userId
  ) {
    return get(
      params,
      BASE_ENDPOINT,
      Map.of("query", String.format("userId==%s", userId))
    );
  }

  public Optional<JsonObject> createPermissionsUser(
    OkapiConnectionParams okapiConnectionParams,
    PermissionUser permissionUser
  ) {
    return post(okapiConnectionParams, BASE_ENDPOINT, permissionUser);
  }

  public Optional<JsonObject> updatePermissionsUser(
    OkapiConnectionParams okapiConnectionParams,
    PermissionUser permissionUser
  ) {
    return put(
      okapiConnectionParams,
      String.format(BASE_ENDPOINT_WITH_ID, permissionUser.getId()),
      permissionUser
    );
  }
}
