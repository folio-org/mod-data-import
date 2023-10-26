package org.folio.service.auth;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.service.auth.PermissionsClient.PermissionUser;
import org.folio.service.auth.UsersClient.User;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;

@RunWith(VertxUnitRunner.class)
public class SystemUserAuthServiceTest {

  @Mock
  AuthClient authClient;

  @Mock
  PermissionsClient permissionsClient;

  @Mock
  UsersClient usersClient;

  ClassPathResource resource = new ClassPathResource("permissions.txt");
  ClassPathResource resourceOptional = new ClassPathResource(
    "permissions-optional.txt"
  );

  SystemUserAuthServiceTestProxy service;

  OkapiConnectionParams params = new OkapiConnectionParams(
    Map.of("x-okapi-tenant", "tenant"),
    null
  );

  @Before
  public void setup() {
    // open mocks for class
    MockitoAnnotations.openMocks(this);

    service =
      new SystemUserAuthServiceTestProxy(
        authClient,
        permissionsClient,
        usersClient,
        "username",
        "password",
        true,
        resource,
        resourceOptional
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
  public void testConstructionPasswordCheck() {
    // enabled case with password, should be successful
    new SystemUserAuthService(
      authClient,
      permissionsClient,
      usersClient,
      "username",
      "password",
      true,
      resource,
      resourceOptional
    );

    // disabled case with password, should be successful
    new SystemUserAuthService(
      authClient,
      permissionsClient,
      usersClient,
      "username",
      "password",
      false,
      resource,
      resourceOptional
    );

    // disabled case with null password, should be successful
    new SystemUserAuthService(
      authClient,
      permissionsClient,
      usersClient,
      "username",
      null,
      false,
      resource,
      resourceOptional
    );

    // disabled case with empty password, should be successful
    new SystemUserAuthService(
      authClient,
      permissionsClient,
      usersClient,
      "username",
      "",
      false,
      resource,
      resourceOptional
    );

    // enabled case with empty password, should fail
    assertThrows(
      IllegalArgumentException.class,
      () ->
        new SystemUserAuthService(
          authClient,
          permissionsClient,
          usersClient,
          "username",
          "",
          true,
          resource,
          resourceOptional
        )
    );

    // enabled case with null password, should fail
    assertThrows(
      IllegalArgumentException.class,
      () ->
        new SystemUserAuthService(
          authClient,
          permissionsClient,
          usersClient,
          "username",
          null,
          true,
          resource,
          resourceOptional
        )
    );
  }

  @Test
  public void testDisabled() {
    SystemUserAuthService test = new SystemUserAuthService(
      authClient,
      permissionsClient,
      usersClient,
      "username",
      "password",
      false,
      resource,
      resourceOptional
    );

    assertThrows(
      IllegalStateException.class,
      () -> test.initializeSystemUser(null)
    );
    assertThrows(IllegalStateException.class, () -> test.getAuthToken(null));
  }

  @Test
  public void testInvalidPermissionFileLoading() {
    SystemUserAuthService testService = new SystemUserAuthService(
      null,
      null,
      null,
      null,
      "password",
      true,
      new ClassPathResource("this-file-does-not-exist"),
      new ClassPathResource("this-file-does-not-exist")
    );

    // fallback results in empty list
    assertThat(testService.getPermissionsList(), is(empty()));
    assertThat(testService.getOptionalPermissionsList(), is(empty()));
  }

  @Test
  public void testChangedCredentials(TestContext context) {
    User response = new User();
    response.setId("user-id");

    when(usersClient.getUserByUsername(any(), eq("username")))
      .thenReturn(Optional.of(response));

    PermissionUser permissionUser = new PermissionUser();
    permissionUser.setPermissions(service.getPermissionsList());
    when(permissionsClient.getPermissionsUserByUserId(any(), eq("user-id")))
      .thenReturn(Optional.of(permissionUser));

    // first attempt: invalid
    // second attempt (after reset): valid
    when(authClient.login(any(), any()))
      .thenReturn(
        Future.failedFuture(
          new NoSuchElementException(
            "test simulating the credentials were invalid"
          )
        )
      )
      .thenReturn(Future.succeededFuture("test token"));

    doNothing().when(authClient).deleteCredentials(any(), eq("user-id"));
    doNothing().when(authClient).saveCredentials(any(), any());

    doAnswer(invocation -> {
        assertThat(invocation.<User>getArgument(1).isActive(), is(true));
        return null;
      })
      .when(usersClient)
      .updateUser(any(), any());

    service
      .initializeSystemUser(Map.of("x-okapi-tenant", "tenant"))
      .onComplete(
        context.asyncAssertSuccess(v -> {
          verify(usersClient, times(1))
            .getUserByUsername(any(), eq("username"));
          verify(permissionsClient, times(1))
            .getPermissionsUserByUserId(any(), eq("user-id"));
          verify(authClient, times(1)).deleteCredentials(any(), eq("user-id"));
          verify(authClient, times(1)).saveCredentials(any(), any());
          verify(usersClient, times(1)).updateUser(any(), any());
          verify(authClient, times(2)).login(any(), any());
          verifyNoMoreInteractions(authClient);
          verifyNoMoreInteractions(permissionsClient);
          verifyNoMoreInteractions(usersClient);
        })
      );
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
      boolean splitEnabled,
      Resource permissionsResource,
      Resource permissionsResourceOptional
    ) {
      super(
        authClient,
        permissionsClient,
        usersClient,
        username,
        password,
        splitEnabled,
        permissionsResource,
        permissionsResourceOptional
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
