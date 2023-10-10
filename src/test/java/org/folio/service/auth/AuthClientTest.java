package org.folio.service.auth;

import static com.github.tomakehurst.wiremock.client.WireMock.anyRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.badRequest;
import static com.github.tomakehurst.wiremock.client.WireMock.created;
import static com.github.tomakehurst.wiremock.client.WireMock.delete;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.exactly;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.serverError;
import static com.github.tomakehurst.wiremock.client.WireMock.urlMatching;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.util.Map;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.folio.service.auth.AuthClient.LoginCredentials;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class AuthClientTest {

  private static final String LOGIN_ENDPOINT = "/authn/login-with-expiry";
  private static final String CREDENTIALS_ENDPOINT = "/authn/credentials";

  AuthClient client = new AuthClient(Vertx.vertx());

  LoginCredentials testLoginCredentials = LoginCredentials
    .builder()
    .tenant("tenant")
    .username("username")
    .password("password")
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
  public void testLogin(TestContext context) {
    mockServer.stubFor(
      post(LOGIN_ENDPOINT)
        .withRequestBody(
          equalToJson(
            JsonObject
              .of("username", "username", "password", "password")
              .toString()
          )
        )
        .willReturn(
          WireMock.created().withHeader("Set-Cookie", "folioAccessToken=result")
        )
    );

    client
      .login(params, testLoginCredentials)
      .onComplete(
        context.asyncAssertSuccess(token -> {
          assertThat(token, is("result"));

          mockServer.verify(
            exactly(1),
            anyRequestedFor(urlMatching(LOGIN_ENDPOINT))
          );
        })
      );
  }

  @Test
  public void testLoginError(TestContext context) {
    mockServer.stubFor(post(LOGIN_ENDPOINT).willReturn(badRequest()));

    client
      .login(params, testLoginCredentials)
      .onComplete(
        context.asyncAssertFailure(v -> {
          mockServer.verify(
            exactly(1),
            anyRequestedFor(urlMatching(LOGIN_ENDPOINT))
          );
        })
      );
  }

  @Test
  public void testSaveCredentials() {
    mockServer.stubFor(
      post(CREDENTIALS_ENDPOINT)
        .withRequestBody(
          equalToJson(JsonObject.mapFrom(testLoginCredentials).toString())
        )
        .willReturn(created())
    );

    client.saveCredentials(params, testLoginCredentials);

    mockServer.verify(
      exactly(1),
      anyRequestedFor(urlMatching(CREDENTIALS_ENDPOINT))
    );
  }

  @Test
  public void testSaveCredentialsBadResponse() {
    mockServer.stubFor(
      post(CREDENTIALS_ENDPOINT)
        .withRequestBody(
          equalToJson(JsonObject.mapFrom(testLoginCredentials).toString())
        )
        .willReturn(serverError())
    );

    assertThrows(
      IllegalStateException.class,
      () -> client.saveCredentials(params, testLoginCredentials)
    );

    mockServer.verify(
      exactly(1),
      anyRequestedFor(urlMatching(CREDENTIALS_ENDPOINT))
    );
  }

  @Test
  public void testDeleteCredentials() {
    mockServer.stubFor(
      delete(urlPathMatching(CREDENTIALS_ENDPOINT))
        .withQueryParam("userId", equalTo("test-id"))
        .withRequestBody(
          equalToJson(JsonObject.mapFrom(testLoginCredentials).toString())
        )
        .willReturn(created())
    );

    client.deleteCredentials(params, "test-id");

    mockServer.verify(
      exactly(1),
      anyRequestedFor(urlPathMatching(CREDENTIALS_ENDPOINT))
    );
  }

  @Test
  public void testDeleteCredentialsBadResponse() {
    mockServer.stubFor(
      delete(urlPathMatching(CREDENTIALS_ENDPOINT))
        .withQueryParam("userId", equalTo("test-id"))
        .withRequestBody(
          equalToJson(JsonObject.mapFrom(testLoginCredentials).toString())
        )
        .willReturn(serverError())
    );

    // should fail silently
    client.deleteCredentials(params, "test-id");

    mockServer.verify(
      exactly(1),
      anyRequestedFor(urlPathMatching(CREDENTIALS_ENDPOINT))
    );
  }
}
