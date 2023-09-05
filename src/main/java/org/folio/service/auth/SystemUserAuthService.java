package org.folio.service.auth;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.service.auth.AuthClient.LoginCredentials;
import org.folio.service.auth.PermissionsClient.PermissionUser;
import org.folio.service.auth.UsersClient.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;

@Service
public class SystemUserAuthService {

  private static final Logger LOGGER = LogManager.getLogger();

  private AuthClient authClient;
  private PermissionsClient permissionsClient;
  private UsersClient usersClient;

  private String username;
  private String password;

  @Getter
  private List<String> permissionsList;

  @Autowired
  public SystemUserAuthService(
    AuthClient authClient,
    PermissionsClient permissionsClient,
    UsersClient usersClient,
    @Value(
      "${SYSTEM_PROCESSING_USERNAME:data-import-system-user}"
    ) String username,
    @Value(
      "${SYSTEM_PROCESSING_PASSWORD:data-import-system-user}"
    ) String password,
    @Value("classpath:permissions.txt") Resource permissionsResource
  ) {
    this.authClient = authClient;
    this.permissionsClient = permissionsClient;
    this.usersClient = usersClient;

    this.username = username;
    this.password = password;

    try {
      this.permissionsList =
        Arrays
          .stream(
            IOUtils
              .toString(
                permissionsResource.getInputStream(),
                StandardCharsets.UTF_8
              )
              .split("\n")
          )
          // account for newlines/whitespace/etc
          .filter(str -> !str.isBlank())
          .map(String::trim)
          .collect(Collectors.toUnmodifiableList());
    } catch (IOException e) {
      LOGGER.error("Could not read permissions: ", e);
      this.permissionsList = Collections.unmodifiableList(new ArrayList<>());
    }
  }

  public void initializeSystemUser(Map<String, String> headers) {
    OkapiConnectionParams okapiConnectionParams = new OkapiConnectionParams(
      headers,
      null
    );

    User user = getOrCreateSystemUserFromApi(okapiConnectionParams);
    validatePermissions(okapiConnectionParams, user);
    getAuthToken(okapiConnectionParams);

    LOGGER.info("System user created successfully!");
  }

  public String getAuthToken(OkapiConnectionParams okapiConnectionParams) {
    LOGGER.info("Attempting {}", getLoginCredentials(okapiConnectionParams));

    return authClient.login(
      okapiConnectionParams,
      getLoginCredentials(okapiConnectionParams)
    );
  }

  protected User getOrCreateSystemUserFromApi(
    OkapiConnectionParams okapiConnectionParams
  ) {
    LOGGER.info(
      "Checking for username {} in tenant {}",
      username,
      okapiConnectionParams.getTenantId()
    );

    Optional<User> user = usersClient.getUserByUsername(
      okapiConnectionParams,
      username
    );

    return user.orElseGet(() -> {
      LOGGER.info(
        "Creating system user {} in tenant {}",
        username,
        okapiConnectionParams.getTenantId()
      );

      User result = usersClient.createUser(
        okapiConnectionParams,
        createSystemUserEntity()
      );

      authClient.saveCredentials(
        okapiConnectionParams,
        getLoginCredentials(okapiConnectionParams)
      );

      return result;
    });
  }

  protected PermissionUser validatePermissions(
    OkapiConnectionParams okapiConnectionParams,
    User user
  ) {
    Optional<PermissionUser> permissionUser = permissionsClient.getPermissionsUserByUserId(
      okapiConnectionParams,
      user.getId()
    );

    if (permissionUser.isEmpty()) {
      LOGGER.info(
        "Creating permissions for system user {} in tenant {}",
        user.getId(),
        okapiConnectionParams.getTenantId()
      );

      PermissionUser payload = PermissionUser
        .builder()
        .id(UUID.randomUUID().toString())
        .userId(user.getId())
        .permissions(this.permissionsList)
        .build();

      return permissionsClient.createPermissionsUser(
        okapiConnectionParams,
        payload
      );
    } else {
      Set<String> missingPermissions = new HashSet<>(this.permissionsList);
      missingPermissions.removeAll(permissionUser.get().getPermissions());

      if (!missingPermissions.isEmpty()) {
        LOGGER.warn(
          "Permissions {} are missing for system user {}; adding them...",
          missingPermissions,
          user.getId()
        );

        PermissionUser payload = permissionUser.get();
        payload.setPermissions(this.permissionsList);

        return permissionsClient.updatePermissionsUser(
          okapiConnectionParams,
          payload
        );
      } else {
        LOGGER.info("System user's permissions look good");
        return permissionUser.get();
      }
    }
  }

  protected LoginCredentials getLoginCredentials(
    OkapiConnectionParams okapiConnectionParams
  ) {
    return LoginCredentials
      .builder()
      .username(username)
      .password(password)
      .tenant(okapiConnectionParams.getTenantId())
      .build();
  }

  protected User createSystemUserEntity() {
    return User
      .builder()
      .id(UUID.randomUUID().toString())
      .active(true)
      .username(username)
      .personal(User.Personal.builder().lastName("SystemDataImport").build())
      .build();
  }
}
