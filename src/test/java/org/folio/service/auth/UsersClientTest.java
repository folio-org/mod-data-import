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
import static com.github.tomakehurst.wiremock.client.WireMock.serverError;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import java.util.Map;
import java.util.NoSuchElementException;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.service.auth.UsersClient.User;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class UsersClientTest {

  private static final String BASE_ENDPOINT = "/users";

  UsersClient client = new UsersClient();

  User testUserRequest = User.builder().id("test").build();
  User testUserResponse = User.builder().id("test-response").build();

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
  public void testGetByUsername() {
    mockServer.stubFor(
      get(urlPathEqualTo(BASE_ENDPOINT))
        .withQueryParam("query", equalTo("username=\"test\""))
        .willReturn(
          okJson(
            JsonObject
              .of(
                "users",
                JsonArray.of(
                  JsonObject.mapFrom(testUserResponse),
                  // should return first result
                  JsonObject.mapFrom(testUserRequest)
                )
              )
              .toString()
          )
        )
    );

    assertThat(
      client.getUserByUsername(params, "test").get(),
      is(testUserResponse)
    );

    mockServer.verify(
      exactly(1),
      anyRequestedFor(urlPathEqualTo(BASE_ENDPOINT))
    );
  }

  @Test
  public void testGetByUsernameEmpty() {
    mockServer.stubFor(
      get(urlPathEqualTo(BASE_ENDPOINT))
        .withQueryParam("query", equalTo("username=\"test\""))
        .willReturn(okJson(JsonObject.of("users", new JsonArray()).toString()))
    );

    assertThat(client.getUserByUsername(params, "test").isEmpty(), is(true));

    mockServer.verify(
      exactly(1),
      anyRequestedFor(urlPathEqualTo(BASE_ENDPOINT))
    );
  }

  @Test
  public void testGetByUsernameError() {
    mockServer.stubFor(
      get(urlPathEqualTo(BASE_ENDPOINT))
        .withQueryParam("query", equalTo("username=\"test\""))
        .willReturn(serverError())
    );

    assertThrows(
      NoSuchElementException.class,
      () -> client.getUserByUsername(params, "test")
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
          equalToJson(JsonObject.mapFrom(testUserRequest).toString())
        )
        .willReturn(ok(JsonObject.mapFrom(testUserResponse).toString()))
    );

    assertThat(
      client.createUser(params, testUserRequest),
      is(testUserResponse)
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
          equalToJson(JsonObject.mapFrom(testUserRequest).toString())
        )
        .willReturn(badRequest())
    );

    assertThrows(
      NoSuchElementException.class,
      () -> client.createUser(params, testUserRequest)
    );

    mockServer.verify(
      exactly(1),
      anyRequestedFor(urlPathEqualTo(BASE_ENDPOINT))
    );
  }
}
