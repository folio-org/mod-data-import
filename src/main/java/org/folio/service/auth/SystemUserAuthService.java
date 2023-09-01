package org.folio.service.auth;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.service.auth.AuthClient.LoginCredentials;
import org.folio.service.auth.PermissionsClient.PermissionUser;
import org.folio.service.auth.UsersClient.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class SystemUserAuthService {

  private static final Logger LOGGER = LogManager.getLogger();

  public static final List<String> PERMISSIONS = Collections.unmodifiableList(
    Arrays.asList(
      "change-manager.jobexecutions.get",
      "change-manager.jobexecutions.put",
      "mapping-metadata.get",
      "source-storage.records.get",
      "source-storage.snapshots.get",
      "source-storage.snapshots.put",
      "users.collection.get",
      "inventory-storage.identifier-types.collection.get",
      "inventory-storage.classification-types.collection.get",
      "inventory-storage.instance-types.collection.get",
      "inventory-storage.electronic-access-relationships.collection.get",
      "inventory-storage.instance-formats.collection.get",
      "inventory-storage.contributor-types.collection.get",
      "inventory-storage.contributor-name-types.collection.get",
      "inventory-storage.instance-note-types.collection.get",
      "inventory-storage.alternative-title-types.collection.get",
      "inventory-storage.modes-of-issuance.collection.get",
      "inventory-storage.item-damaged-statuses.collection.get",
      "inventory-storage.instance-statuses.collection.get",
      "inventory-storage.nature-of-content-terms.collection.get",
      "inventory-storage.instance-relationship-types.collection.get",
      "inventory-storage.holdings-types.collection.get",
      "inventory-storage.holdings-note-types.collection.get",
      "inventory-storage.holdings-sources.collection.get",
      "inventory-storage.ill-policies.collection.get",
      "inventory-storage.call-number-types.collection.get",
      "inventory-storage.statistical-codes.collection.get",
      "inventory-storage.statistical-code-types.collection.get",
      "inventory-storage.item-note-types.collection.get",
      "inventory-storage.authority-note-types.collection.get",
      "inventory-storage.authority-source-files.collection.get",
      "inventory-storage.items.collection.get",
      "inventory-storage.items.item.post",
      "inventory-storage.items.item.put",
      "inventory-storage.material-types.item.get",
      "inventory-storage.material-types.collection.get",
      "inventory-storage.loan-types.item.get",
      "inventory-storage.loan-types.collection.get",
      "inventory-storage.locations.item.get",
      "inventory-storage.locations.collection.get",
      "inventory-storage.holdings.collection.get",
      "inventory-storage.holdings.item.get",
      "inventory-storage.holdings.item.post",
      "inventory-storage.holdings.item.put",
      "inventory-storage.instances.collection.get",
      "inventory-storage.instances.item.get",
      "inventory-storage.instances.item.post",
      "inventory-storage.instances.item.put",
      "inventory-storage.authorities.collection.get",
      "inventory-storage.authorities.item.get",
      "inventory-storage.authorities.item.post",
      "inventory-storage.authorities.item.put",
      "inventory-storage.preceding-succeeding-titles.item.post",
      "inventory-storage.preceding-succeeding-titles.collection.get",
      "inventory-storage.preceding-succeeding-titles.item.get",
      "inventory-storage.preceding-succeeding-titles.item.put",
      "inventory-storage.preceding-succeeding-titles.item.delete",
      "converter-storage.field-protection-settings.get",
      "converter-storage.jobprofilesnapshots.get",
      "invoice-storage.invoices.item.post",
      "invoice-storage.invoice-number.get",
      "acquisitions-units.units.collection.get",
      "acquisitions-units.memberships.collection.get",
      "invoice-storage.invoice-line-number.get",
      "invoice-storage.invoices.item.get",
      "invoice-storage.invoices.item.put",
      "invoice-storage.invoice-lines.item.post",
      "invoice-storage.invoice-lines.item.put",
      "invoice-storage.invoice-lines.collection.get",
      "acquisitions-units.units.collection.get",
      "acquisitions-units.memberships.collection.get",
      "orders.po-lines.item.get",
      "orders.po-lines.collection.get",
      "orders-storage.order-invoice-relationships.collection.get",
      "orders-storage.order-invoice-relationships.item.post",
      "finance.exchange-rate.item.get",
      "finance.expense-classes.collection.get",
      "finance.funds.budget.item.get",
      "finance.fiscal-years.item.get",
      "finance.ledgers.collection.get",
      "finance.transactions.collection.get",
      "finance-storage.budgets.collection.get",
      "finance-storage.budget-expense-classes.collection.get",
      "finance-storage.fiscal-years.item.get",
      "modperms.orders.item.post",
      "modperms.orders.item.put",
      "orders-storage.purchase-orders.item.get",
      "orders-storage.po-line-number.get",
      "orders-storage.po-lines.collection.get",
      "orders-storage.po-lines.item.post",
      "orders-storage.alerts.item.post",
      "orders-storage.reporting-codes.item.post",
      "orders-storage.configuration.prefixes.collection.get",
      "orders-storage.configuration.suffixes.collection.get",
      "configuration.entries.collection.get",
      "acquisitions-units-storage.units.collection.get",
      "acquisitions-units-storage.memberships.collection.get",
      "isbn-utils.convert-to-13.get",
      "instance-authority-links.instances.collection.get",
      "instance-authority-links.instances.collection.put"
    )
  );

  private AuthClient authClient;
  private PermissionsClient permissionsClient;
  private UsersClient usersClient;

  private String username;
  private String password;

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
    ) String password
  ) {
    this.authClient = authClient;
    this.permissionsClient = permissionsClient;
    this.usersClient = usersClient;

    this.username = username;
    this.password = password;
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
        .permissions(PERMISSIONS)
        .build();

      return permissionsClient.createPermissionsUser(
        okapiConnectionParams,
        payload
      );
    } else {
      Set<String> missingPermissions = new HashSet<>(PERMISSIONS);
      missingPermissions.removeAll(permissionUser.get().getPermissions());

      if (!missingPermissions.isEmpty()) {
        LOGGER.warn(
          "Permissions {} are missing for system user {}; adding them...",
          missingPermissions,
          user.getId()
        );

        PermissionUser payload = permissionUser.get();
        payload.setPermissions(PERMISSIONS);

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
