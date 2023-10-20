package org.folio.service.auth;

import io.vertx.core.Future;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import lombok.Getter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
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
  private static final String SYSTEM_USER_TYPE = "system";

  private boolean fileSplittingEnabled;

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
    @Value("${SYSTEM_PROCESSING_PASSWORD:}") String password,
    @Value("${SPLIT_FILES_ENABLED:false}") boolean fileSplittingEnabled,
    @Value("classpath:permissions.txt") Resource permissionsResource
  ) {
    // ensure password is set properly
    if (fileSplittingEnabled && StringUtils.isEmpty(password)) {
      throw new IllegalArgumentException(
        "System user password must be provided when file splitting is enabled (env variable SYSTEM_PROCESSING_PASSWORD)"
      );
    }

    this.fileSplittingEnabled = fileSplittingEnabled;

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
          .toList();
    } catch (IOException e) {
      LOGGER.error("Could not read permissions: ", e);
      this.permissionsList = Collections.unmodifiableList(new ArrayList<>());
    }
  }

  private void ensureSplittingEnabled() {
    if (!fileSplittingEnabled) {
      throw LOGGER.throwing(
        new IllegalStateException(
          "System user is not available unless file splitting is enabled"
        )
      );
    }
  }

  public void initializeSystemUser(Map<String, String> headers) {
    ensureSplittingEnabled();

    OkapiConnectionParams okapiConnectionParams = new OkapiConnectionParams(
      headers,
      null
    );

    User user = getOrCreateSystemUserFromApi(okapiConnectionParams);
    validatePermissions(okapiConnectionParams, user);
    try {
      getAuthToken(okapiConnectionParams);
    } catch (NoSuchElementException e) {
      LOGGER.error("Could not get auth token: ", e);

      LOGGER.info(
        "This may be due to a password change...resetting system user password"
      );

      recoverSystemUserAfterPasswordChange(okapiConnectionParams, user);
    }

    LOGGER.info("System user created/found successfully!");
  }

  protected void recoverSystemUserAfterPasswordChange(
    OkapiConnectionParams params,
    User user
  ) {
    LOGGER.info(
      "Attempting to delete existing credentials for user {}...",
      user.getId()
    );
    authClient.deleteCredentials(params, user.getId());

    LOGGER.info("Saving new credentials...");
    authClient.saveCredentials(params, getLoginCredentials(params));

    LOGGER.info("Marking user as active...");
    user.setActive(true);
    usersClient.updateUser(params, user);

    LOGGER.info("Verifying we can login...");
    getAuthToken(params);
  }

  public Future<String> getAuthToken(
    OkapiConnectionParams okapiConnectionParams
  ) {
    ensureSplittingEnabled();

    LOGGER.info("Attempting {}", getLoginCredentials(okapiConnectionParams));

    return authClient
      .login(okapiConnectionParams, getLoginCredentials(okapiConnectionParams))
      .onFailure((Throwable err) ->
        LOGGER.error("Unable to login as system user", err)
      )
      .onSuccess((String v) -> LOGGER.info("Logged in successfully!"));
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
      .type(SYSTEM_USER_TYPE)
      .personal(User.Personal.builder().lastName("SystemDataImport").build())
      .build();
  }
}
