package org.folio.service.auth;

import static com.github.tomakehurst.wiremock.client.WireMock.anyRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.badRequest;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.exactly;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.ok;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static com.github.tomakehurst.wiremock.client.WireMock.serverError;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathTemplate;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.Arrays;
import java.util.Map;
import java.util.NoSuchElementException;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.service.auth.PermissionsClient.PermissionUser;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class PermissionsClientTest {

  private static final String BASE_ENDPOINT = "/perms/users";
  private static final String BASE_ENDPOINT_WITH_ID = "/perms/users/{userId}";

  PermissionsClient client = new PermissionsClient();

  PermissionUser testPermissionUserRequest = PermissionUser
    .builder()
    .id("test")
    .permissions(Arrays.asList("perm1", "perm2"))
    .build();

  PermissionUser testPermissionUserResponse = PermissionUser
    .builder()
    .id("test-response")
    .permissions(Arrays.asList("perm1", "perm2", "perm3"))
    .build();

  public WireMockServer mockServer = new WireMockServer(
    WireMockConfiguration
      .wireMockConfig()
      .dynamicPort()
      .notifier(new Slf4jNotifier(true))
  );

  // set in @Before
  OkapiConnectionParams params;

  @Before
  public void setup() {
    mockServer.start();

    params =
      new OkapiConnectionParams(
        Map.of(
          "x-okapi-url",
          mockServer.baseUrl(),
          "x-okapi-tenant",
          "tenant",
          "x-okapi-token",
          "token"
        ),
        null
      );
  }

  @After
  public void teardown() {
    mockServer.stop();
  }

  @Test
  public void testGetById() {
    mockServer.stubFor(
      get(urlPathEqualTo(BASE_ENDPOINT))
        .withQueryParam("query", equalTo("userId==test"))
        .willReturn(
          okJson(
            JsonObject
              .of(
                "permissionUsers",
                JsonArray.of(
                  JsonObject.mapFrom(testPermissionUserResponse),
                  // should return first result
                  JsonObject.mapFrom(testPermissionUserRequest)
                )
              )
              .toString()
          )
        )
    );

    assertThat(
      client.getPermissionsUserByUserId(params, "test").get(),
      is(testPermissionUserResponse)
    );

    mockServer.verify(
      exactly(1),
      anyRequestedFor(urlPathEqualTo(BASE_ENDPOINT))
    );
  }

  @Test
  public void testGetByIdEmpty() {
    mockServer.stubFor(
      get(urlPathEqualTo(BASE_ENDPOINT))
        .withQueryParam("query", equalTo("userId==test"))
        .willReturn(
          okJson(JsonObject.of("permissionUsers", new JsonArray()).toString())
        )
    );

    assertThat(
      client.getPermissionsUserByUserId(params, "test").isEmpty(),
      is(true)
    );

    mockServer.verify(
      exactly(1),
      anyRequestedFor(urlPathEqualTo(BASE_ENDPOINT))
    );
  }

  @Test
  public void testGetByIdError() {
    mockServer.stubFor(
      get(urlPathEqualTo(BASE_ENDPOINT))
        .withQueryParam("query", equalTo("userId==test"))
        .willReturn(serverError())
    );

    assertThrows(
      NoSuchElementException.class,
      () -> client.getPermissionsUserByUserId(params, "test")
    );

    mockServer.verify(
      exactly(1),
      anyRequestedFor(urlPathEqualTo(BASE_ENDPOINT))
    );
  }

  @Test
  public void testCreate() {
    mockServer.stubFor(
      post(urlPathEqualTo(BASE_ENDPOINT))
        .withRequestBody(
          equalToJson(JsonObject.mapFrom(testPermissionUserRequest).toString())
        )
        .willReturn(
          ok(JsonObject.mapFrom(testPermissionUserResponse).toString())
        )
    );

    assertThat(
      client.createPermissionsUser(params, testPermissionUserRequest),
      is(testPermissionUserResponse)
    );

    mockServer.verify(
      exactly(1),
      anyRequestedFor(urlPathEqualTo(BASE_ENDPOINT))
    );
  }

  @Test
  public void testCreateError() {
    mockServer.stubFor(
      post(urlPathEqualTo(BASE_ENDPOINT))
        .withRequestBody(
          equalToJson(JsonObject.mapFrom(testPermissionUserRequest).toString())
        )
        .willReturn(badRequest())
    );

    assertThrows(
      NoSuchElementException.class,
      () -> client.createPermissionsUser(params, testPermissionUserRequest)
    );

    mockServer.verify(
      exactly(1),
      anyRequestedFor(urlPathEqualTo(BASE_ENDPOINT))
    );
  }

  @Test
  public void testUpdate() {
    mockServer.stubFor(
      put(urlPathTemplate(BASE_ENDPOINT_WITH_ID))
        .withPathParam("userId", equalTo("test"))
        .withRequestBody(
          equalToJson(JsonObject.mapFrom(testPermissionUserRequest).toString())
        )
        .willReturn(
          okJson(JsonObject.mapFrom(testPermissionUserResponse).toString())
        )
    );

    assertThat(
      client.updatePermissionsUser(params, testPermissionUserRequest),
      is(testPermissionUserResponse)
    );

    mockServer.verify(
      exactly(1),
      anyRequestedFor(urlPathTemplate(BASE_ENDPOINT_WITH_ID))
    );
  }

  @Test
  public void testUpdateError() {
    mockServer.stubFor(
      put(urlPathTemplate(BASE_ENDPOINT_WITH_ID))
        .withPathParam("userId", equalTo("test"))
        .withRequestBody(
          equalToJson(JsonObject.mapFrom(testPermissionUserRequest).toString())
        )
        .willReturn(badRequest())
    );

    assertThrows(
      NoSuchElementException.class,
      () -> client.updatePermissionsUser(params, testPermissionUserRequest)
    );

    mockServer.verify(
      exactly(1),
      anyRequestedFor(urlPathTemplate(BASE_ENDPOINT_WITH_ID))
    );
  }
}
