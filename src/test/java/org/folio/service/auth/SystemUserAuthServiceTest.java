package org.folio.service.auth;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
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
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

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
        "password",
        new ClassPathResource("permissions.txt")
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
      .permissions(service.getPermissionsList())
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
      .permissions(service.getPermissionsList().subList(0, 5))
      .build();

    when(permissionsClient.getPermissionsUserByUserId(params, "user-id"))
      .thenReturn(Optional.of(initialResponse));
    when(permissionsClient.updatePermissionsUser(eq(params), any()))
      .thenAnswer(input -> {
        PermissionUser pu = (PermissionUser) input.getArgument(1);

        assertThat(pu.getPermissions(), is(service.getPermissionsList()));

        return pu;
      });

    // updated version should have all
    assertThat(
      service.validatePermissions(params, user).getPermissions(),
      is(service.getPermissionsList())
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

        assertThat(pu.getPermissions(), is(service.getPermissionsList()));

        return pu;
      });

    // created version should have all
    assertThat(
      service.validatePermissions(params, user).getPermissions(),
      is(service.getPermissionsList())
    );

    verify(permissionsClient, times(1))
      .getPermissionsUserByUserId(params, "user-id");
    verify(permissionsClient, times(1))
      .createPermissionsUser(eq(params), any());

    verifyNoMoreInteractions(authClient);
    verifyNoMoreInteractions(permissionsClient);
    verifyNoMoreInteractions(usersClient);
  }

  @Test
  public void testPermissionFileLoading() {
    assertThat(
      service.getPermissionsList(),
      contains(
        "test-perm-1",
        "test-perm-2",
        "test-perm-3",
        "test-perm-4",
        "test-perm-5",
        "test-perm-6",
        "test-perm-7",
        "test-perm-8",
        "test-perm-9",
        "test-perm-10"
      )
    );
  }

  @Test
  public void testInvalidPermissionFileLoading() {
    SystemUserAuthService testService = new SystemUserAuthService(
      null,
      null,
      null,
      null,
      null,
      new ClassPathResource("this-file-does-not-exist")
    );

    // fallback results in empty list
    assertThat(testService.getPermissionsList(), is(empty()));
  }

  // allow access to private methods
  private static class SystemUserAuthServiceTestProxy
    extends SystemUserAuthService {

    public SystemUserAuthServiceTestProxy(
      AuthClient authClient,
      PermissionsClient permissionsClient,
      UsersClient usersClient,
      String username,
      String password,
      Resource permissionsResource
    ) {
      super(
        authClient,
        permissionsClient,
        usersClient,
        username,
        password,
        permissionsResource
      );
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
