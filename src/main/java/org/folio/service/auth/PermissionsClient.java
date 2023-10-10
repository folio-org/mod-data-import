package org.folio.service.auth;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import io.vertx.core.json.JsonObject;
import java.util.List;
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
public class PermissionsClient extends ApiClient {

  private static final String BASE_ENDPOINT = "perms/users";
  private static final String BASE_ENDPOINT_WITH_ID = "perms/users/%s";

  public Optional<PermissionUser> getPermissionsUserByUserId(
    OkapiConnectionParams params,
    String userId
  ) {
    return sendRequest(
      new HttpGet(),
      params,
      BASE_ENDPOINT,
      Map.of("query", String.format("userId==%s", userId)),
      ApiClient::getResponseEntity
    )
      .orElseThrow()
      .getJsonArray("permissionUsers")
      .stream()
      .findFirst()
      .map(o -> (JsonObject) o)
      .map(o -> o.mapTo(PermissionUser.class));
  }

  public PermissionUser createPermissionsUser(
    OkapiConnectionParams okapiConnectionParams,
    PermissionUser permissionUser
  ) {
    return sendRequest(
      new HttpPost(),
      okapiConnectionParams,
      BASE_ENDPOINT,
      null,
      permissionUser,
      ApiClient::getResponseEntity
    )
      .orElseThrow()
      .mapTo(PermissionUser.class);
  }

  public PermissionUser updatePermissionsUser(
    OkapiConnectionParams okapiConnectionParams,
    PermissionUser permissionUser
  ) {
    return sendRequest(
      new HttpPut(),
      okapiConnectionParams,
      String.format(BASE_ENDPOINT_WITH_ID, permissionUser.getId()),
      null,
      permissionUser,
      ApiClient::getResponseEntity
    )
      .orElseThrow()
      .mapTo(PermissionUser.class);
  }

  @Data
  @Builder
  @NoArgsConstructor
  @AllArgsConstructor
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class PermissionUser {

    private String id;
    private String userId;
    private List<String> permissions;
  }
}
