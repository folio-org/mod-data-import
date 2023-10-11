package org.folio.service.auth;

import static com.github.tomakehurst.wiremock.client.WireMock.anyRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.equalToJson;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.noValues;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.postRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathEqualTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import io.vertx.core.json.JsonObject;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.InputStreamEntity;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ApiClientTest {

  ApiClientProxy client = new ApiClientProxy();

  public WireMockServer mockServer = new WireMockServer(
    WireMockConfiguration
      .wireMockConfig()
      .dynamicPort()
      .notifier(new Slf4jNotifier(true))
  );

  // set in @Before
  OkapiConnectionParams params;

  OkapiConnectionParams badUriParams = new OkapiConnectionParams(
    Map.of("x-okapi-url", "[test]"),
    null
  );

  OkapiConnectionParams badRequestParams = new OkapiConnectionParams(
    Map.of("x-okapi-url", ""),
    null
  );

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
  public void testNoPayload() {
    // ensure appropriate headers are sent
    mockServer.stubFor(
      get(urlPathEqualTo("/endpoint"))
        .withHeader("x-okapi-tenant", equalTo("tenant"))
        .withHeader("x-okapi-token", equalTo("token"))
        .withHeader("content-type", noValues())
        .willReturn(okJson(new JsonObject().toString()))
    );

    assertThat(
      client
        .sendRequest(
          new HttpGet(),
          params,
          "endpoint",
          null,
          ApiClientProxy::getResponseEntity
        )
        .get()
        .toString(),
      is("{}")
    );

    mockServer.verify(1, anyRequestedFor(urlPathEqualTo("/endpoint")));
  }

  @Test
  public void testPayload() {
    // ensure appropriate headers are sent
    mockServer.stubFor(
      post(urlPathEqualTo("/endpoint"))
        .withQueryParam("foo", equalTo("bar"))
        .withHeader("x-okapi-tenant", equalTo("tenant"))
        .withHeader("x-okapi-token", equalTo("token"))
        .withRequestBody(equalToJson("{\"test\":\"payload\"}"))
        .willReturn(okJson(new JsonObject().toString()))
    );

    client.sendRequest(
      new HttpPost(),
      params,
      "endpoint",
      Map.of("foo", "bar"),
      Map.of("test", "payload"),
      ApiClientProxy::getResponseEntity
    );

    mockServer.verify(
      1,
      postRequestedFor(urlPathEqualTo("/endpoint"))
        .withQueryParam("foo", equalTo("bar"))
        .withHeader("x-okapi-tenant", equalTo("tenant"))
        .withHeader("x-okapi-token", equalTo("token"))
        .withRequestBody(equalToJson("{\"test\":\"payload\"}"))
    );
  }

  @Test
  public void testEmptyPayload() {
    // no content-type = no request body
    mockServer.stubFor(
      post(urlPathEqualTo("/endpoint"))
        .withQueryParam("foo", equalTo("bar"))
        .withHeader("x-okapi-tenant", equalTo("tenant"))
        .withHeader("x-okapi-token", equalTo("token"))
        .withHeader("content-type", noValues())
        .willReturn(okJson(new JsonObject().toString()))
    );

    client.sendRequest(
      new HttpPost(),
      params,
      "endpoint",
      Map.of("foo", "bar"),
      null,
      ApiClientProxy::getResponseEntity
    );

    mockServer.verify(
      1,
      postRequestedFor(urlPathEqualTo("/endpoint"))
        .withQueryParam("foo", equalTo("bar"))
        .withHeader("x-okapi-tenant", equalTo("tenant"))
        .withHeader("x-okapi-token", equalTo("token"))
        .withHeader("content-type", noValues())
    );
  }

  @Test
  public void testUriException() {
    HttpPost request = new HttpPost();
    assertThrows(
      IllegalArgumentException.class,
      () -> client.sendRequest(request, badUriParams, "", null, v -> null)
    );
  }

  @Test
  public void testRequestFailure() {
    HttpGet request = new HttpGet();
    assertThrows(
      UncheckedIOException.class,
      () -> client.sendRequest(request, badRequestParams, "", null, v -> null)
    );
  }

  @Test
  public void testPayloadException() {
    Object obj = new Object();
    HttpPut request = new HttpPut();
    assertThrows(
      IllegalArgumentException.class,
      () ->
        client.sendRequest(request, params, "endpoint", null, obj, v -> null)
    );
  }

  @Test
  public void testResponseEntityGetterSuccess() {
    JsonObject test = JsonObject.of("foo", "bar");

    // ensure appropriate headers are sent
    mockServer.stubFor(
      get(urlPathEqualTo("/endpoint")).willReturn(okJson(test.toString()))
    );

    assertThat(
      client
        .sendRequest(
          new HttpGet(),
          params,
          "endpoint",
          null,
          ApiClientProxy::getResponseEntity
        )
        .get(),
      is(test)
    );
  }

  @Test
  public void testVerifyOk() {
    HttpResponse response = mock(HttpResponse.class);
    StatusLine status = mock(StatusLine.class);

    // bad codes
    Arrays
      .asList(100, 400, 404, 500)
      .stream()
      .forEach(code -> {
        when(status.getStatusCode()).thenReturn(code);
        when(response.getStatusLine()).thenReturn(status);

        assertThat(ApiClientProxy.isResponseOk(response), is(false));
      });

    // good codes
    Arrays
      .asList(200, 201, 204)
      .stream()
      .forEach(code -> {
        when(status.getStatusCode()).thenReturn(code);
        when(response.getStatusLine()).thenReturn(status);

        assertThat(ApiClientProxy.isResponseOk(response), is(true));
      });
  }

  @Test
  public void testResponseEntityGetterNoEntity() {
    HttpResponse response = mock(HttpResponse.class);
    StatusLine status = mock(StatusLine.class);

    when(status.getStatusCode()).thenReturn(200);
    when(response.getStatusLine()).thenReturn(status);
    when(response.getEntity()).thenReturn(null);

    assertThat(ApiClientProxy.getResponseEntity(response).isEmpty(), is(true));
  }

  @Test
  public void testResponseEntityBadEntity() {
    HttpResponse response = mock(HttpResponse.class);
    StatusLine status = mock(StatusLine.class);

    when(status.getStatusCode()).thenReturn(200);
    when(response.getStatusLine()).thenReturn(status);
    when(response.getEntity())
      .thenReturn(
        new InputStreamEntity(
          new InputStream() {
            @Override
            public int read() throws IOException {
              throw new IOException("kaboom");
            }
          }
        )
      );

    assertThrows(
      UncheckedIOException.class,
      () -> ApiClientProxy.getResponseEntity(response)
    );
  }

  private static class ApiClientProxy extends ApiClient {

    @Override
    public <T> T sendRequest(
      HttpRequestBase request,
      OkapiConnectionParams params,
      String endpoint,
      Map<String, String> query,
      Function<HttpResponse, T> responseMapper
    ) {
      return super.sendRequest(
        request,
        params,
        endpoint,
        query,
        responseMapper
      );
    }

    @Override
    public <T> T sendRequest(
      HttpEntityEnclosingRequestBase request,
      OkapiConnectionParams params,
      String endpoint,
      Map<String, String> query,
      Object payload,
      Function<HttpResponse, T> responseMapper
    ) {
      return super.sendRequest(
        request,
        params,
        endpoint,
        query,
        payload,
        responseMapper
      );
    }

    public static Optional<JsonObject> getResponseEntity(
      HttpResponse response
    ) {
      return ApiClient.getResponseEntity(response);
    }

    public static boolean isResponseOk(HttpResponse response) {
      return ApiClient.isResponseOk(response);
    }
  }
}
