package org.folio.service.auth;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Map;
import java.util.Optional;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.service.auth.PermissionsClient.PermissionUser;
import org.folio.service.auth.UsersClient.User;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class SystemUserAuthServiceTest {

  @Mock
  AuthClient authClient;

  @Mock
  PermissionsClient permissionsClient;

  @Mock
  UsersClient usersClient;

  SystemUserAuthServiceTestProxy service;

  OkapiConnectionParams params = new OkapiConnectionParams(
    Map.of("x-okapi-tenant", "tenant"),
    null
  );

  @Before
  public void setup() {
    service =
      new SystemUserAuthServiceTestProxy(
        authClient,
        permissionsClient,
        usersClient,
        "username",
        "password"
      );
  }

  @Test
  public void testGetUserExisting() {
    User response = new User();

    when(usersClient.getUserByUsername(params, "username"))
      .thenReturn(Optional.of(response));

    assertThat(service.getOrCreateSystemUserFromApi(params), is(response));

    verify(usersClient, times(1)).getUserByUsername(params, "username");

    verifyNoMoreInteractions(authClient);
    verifyNoMoreInteractions(permissionsClient);
    verifyNoMoreInteractions(usersClient);
  }

  @Test
  public void testCreateUser() {
    User response = new User();

    when(usersClient.getUserByUsername(params, "username"))
      .thenReturn(Optional.empty());
    when(usersClient.createUser(eq(params), any())).thenReturn(response);

    assertThat(service.getOrCreateSystemUserFromApi(params), is(response));

    verify(authClient, times(1)).saveCredentials(eq(params), any());
    verify(usersClient, times(1)).getUserByUsername(params, "username");
    verify(usersClient, times(1)).createUser(eq(params), any());

    verifyNoMoreInteractions(authClient);
    verifyNoMoreInteractions(permissionsClient);
    verifyNoMoreInteractions(usersClient);
  }

  @Test
  public void testGetPermissionsOk() {
    User user = User.builder().id("user-id").build();

    PermissionUser response = PermissionUser
      .builder()
      .permissions(SystemUserAuthService.PERMISSIONS)
      .build();

    when(permissionsClient.getPermissionsUserByUserId(params, "user-id"))
      .thenReturn(Optional.of(response));

    assertThat(service.validatePermissions(params, user), is(response));

    verify(permissionsClient, times(1))
      .getPermissionsUserByUserId(params, "user-id");

    verifyNoMoreInteractions(authClient);
    verifyNoMoreInteractions(permissionsClient);
    verifyNoMoreInteractions(usersClient);
  }

  @Test
  public void testGetPermissionsAddTo() {
    User user = User.builder().id("user-id").build();

    PermissionUser initialResponse = PermissionUser
      .builder()
      .permissions(SystemUserAuthService.PERMISSIONS.subList(0, 5))
      .build();

    when(permissionsClient.getPermissionsUserByUserId(params, "user-id"))
      .thenReturn(Optional.of(initialResponse));
    when(permissionsClient.updatePermissionsUser(eq(params), any()))
      .thenAnswer(input -> {
        PermissionUser pu = (PermissionUser) input.getArgument(1);

        assertThat(pu.getPermissions(), is(SystemUserAuthService.PERMISSIONS));

        return pu;
      });

    // updated version should have all
    assertThat(
      service.validatePermissions(params, user).getPermissions(),
      is(SystemUserAuthService.PERMISSIONS)
    );

    verify(permissionsClient, times(1))
      .getPermissionsUserByUserId(params, "user-id");
    verify(permissionsClient, times(1))
      .updatePermissionsUser(eq(params), any());

    verifyNoMoreInteractions(authClient);
    verifyNoMoreInteractions(permissionsClient);
    verifyNoMoreInteractions(usersClient);
  }

  @Test
  public void testGetPermissionsCreate() {
    User user = User.builder().id("user-id").build();

    when(permissionsClient.getPermissionsUserByUserId(params, "user-id"))
      .thenReturn(Optional.empty());
    when(permissionsClient.createPermissionsUser(eq(params), any()))
      .thenAnswer(input -> {
        PermissionUser pu = (PermissionUser) input.getArgument(1);

        assertThat(pu.getPermissions(), is(SystemUserAuthService.PERMISSIONS));

        return pu;
      });

    // created version should have all
    assertThat(
      service.validatePermissions(params, user).getPermissions(),
      is(SystemUserAuthService.PERMISSIONS)
    );

    verify(permissionsClient, times(1))
      .getPermissionsUserByUserId(params, "user-id");
    verify(permissionsClient, times(1))
      .createPermissionsUser(eq(params), any());

    verifyNoMoreInteractions(authClient);
    verifyNoMoreInteractions(permissionsClient);
    verifyNoMoreInteractions(usersClient);
  }

  // allow access to private methods
  private static class SystemUserAuthServiceTestProxy
    extends SystemUserAuthService {

    public SystemUserAuthServiceTestProxy(
      AuthClient authClient,
      PermissionsClient permissionsClient,
      UsersClient usersClient,
      String username,
      String password
    ) {
      super(authClient, permissionsClient, usersClient, username, password);
    }

    protected User getOrCreateSystemUserFromApi(
      OkapiConnectionParams okapiConnectionParams
    ) {
      return super.getOrCreateSystemUserFromApi(okapiConnectionParams);
    }

    protected PermissionUser validatePermissions(
      OkapiConnectionParams okapiConnectionParams,
      User user
    ) {
      return super.validatePermissions(okapiConnectionParams, user);
    }
  }
}
