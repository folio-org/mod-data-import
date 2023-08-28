package org.folio.service.auth;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.PermissionUser;
import org.folio.rest.jaxrs.model.Personal;
import org.folio.rest.jaxrs.model.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class SystemUserAuthService {

  private static final Logger LOGGER = LogManager.getLogger();

  private static final List<String> PERMISSIONS = Arrays.asList(
    "change-manager.jobexecutions.get",
    "change-manager.jobexecutions.put",
    "orders.item.unopen"
  );

  private PermissionsClient permissionsClient;
  private UsersClient usersClient;

  private String username;
  private String password;

  @Autowired
  public SystemUserAuthService(
    PermissionsClient permissionsClient,
    UsersClient usersClient,
    @Value(
      "${SYSTEM_PROCESSING_USERNAME:data-import-system-user}"
    ) String username,
    @Value(
      "${SYSTEM_PROCESSING_PASSWORD:data-import-system-user}"
    ) String password
  ) {
    this.permissionsClient = permissionsClient;
    this.usersClient = usersClient;

    this.username = username;
    this.password = password;
  }

  public Future<Void> prepareSystemUser(Map<String, String> headers) {
    OkapiConnectionParams okapiConnectionParams = new OkapiConnectionParams(
      headers,
      null
    );

    User user = getOrCreateSystemUser(okapiConnectionParams);
    validatePermissions(okapiConnectionParams, user);

    return Future.succeededFuture();
  }

  public User getOrCreateSystemUser(
    OkapiConnectionParams okapiConnectionParams
  ) {
    LOGGER.info(
      "Checking for username {} in tenant {}",
      username,
      okapiConnectionParams.getTenantId()
    );
    JsonArray users = usersClient
      .getUserByUsername(okapiConnectionParams, username)
      .orElseThrow()
      .getJsonArray("users");

    if (users.isEmpty()) {
      LOGGER.info(
        "Creating system user {} in tenant {}",
        username,
        okapiConnectionParams.getTenantId()
      );
      return usersClient
        .createUser(okapiConnectionParams, createSystemUserEntity())
        .orElseThrow()
        .mapTo(User.class);
    } else {
      User user = users.getJsonObject(0).mapTo(User.class);

      LOGGER.info("Found system user with ID {}", user.getId());

      return user;
    }
  }

  public PermissionUser validatePermissions(
    OkapiConnectionParams okapiConnectionParams,
    User user
  ) {
    JsonArray permissions = permissionsClient
      .getPermissionsUserByUserId(okapiConnectionParams, user.getId())
      .orElseThrow()
      .getJsonArray("permissionUsers");

    if (permissions.isEmpty()) {
      LOGGER.info(
        "Creating permissions for system user {} in tenant {}",
        user.getId(),
        okapiConnectionParams.getTenantId()
      );

      PermissionUser permissionUser = new PermissionUser()
        .withId(UUID.randomUUID().toString())
        .withUserId(user.getId())
        .withPermissions(PERMISSIONS);

      return permissionsClient
        .createPermissionsUser(okapiConnectionParams, permissionUser)
        .orElseThrow()
        .mapTo(PermissionUser.class);
    } else {
      PermissionUser permissionUser = permissions
        .getJsonObject(0)
        .mapTo(PermissionUser.class);

      Set<String> missingPermissions = new HashSet<>(PERMISSIONS);
      missingPermissions.removeAll(permissionUser.getPermissions());

      if (!missingPermissions.isEmpty()) {
        LOGGER.warn(
          "Permissions {} are missing for system user {}; adding them...",
          missingPermissions,
          user.getId()
        );

        permissionUser.getPermissions().addAll(missingPermissions);

        permissionUser =
          permissionsClient
            .updatePermissionsUser(okapiConnectionParams, permissionUser)
            .orElseThrow()
            .mapTo(PermissionUser.class);
      } else {
        LOGGER.info("System user's permissions look good");
      }

      return permissionUser;
    }
  }

  public User createSystemUserEntity() {
    return new User()
      .withId(UUID.randomUUID().toString())
      .withActive(true)
      .withUsername(username)
      .withPersonal(new Personal().withLastName("SystemDataImport"));
  }
}
