package org.folio.service.auth;

import static com.github.tomakehurst.wiremock.client.WireMock.anyRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
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
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.InputStreamEntity;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ApiClientTest {

  Map<String, String> emptyMap = Map.of();

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
  public void testGet() {
    // ensure appropriate headers are sent
    mockServer.stubFor(
      get(urlPathEqualTo("/endpoint"))
        .withHeader("x-okapi-tenant", equalTo("tenant"))
        .withHeader("x-okapi-token", equalTo("token"))
        .willReturn(okJson(new JsonObject().toString()))
    );

    client.get(params, "endpoint", emptyMap);

    mockServer.verify(1, anyRequestedFor(urlPathEqualTo("/endpoint")));
  }

  @Test
  public void testGetUriException() {
    assertThrows(
      IllegalArgumentException.class,
      () -> client.get(badUriParams, "", emptyMap)
    );
  }

  @Test
  public void testGetRequestFailure() {
    assertThrows(
      UncheckedIOException.class,
      () -> client.get(badRequestParams, "", emptyMap)
    );
  }

  @Test
  public void testPost() {
    // ensure appropriate headers are sent
    mockServer.stubFor(
      post(urlPathEqualTo("/endpoint"))
        .withHeader("x-okapi-tenant", equalTo("tenant"))
        .withHeader("x-okapi-token", equalTo("token"))
        .willReturn(okJson(new JsonObject().toString()))
    );

    client.postOrPut(
      HttpPost::new,
      params,
      "endpoint",
      new HashMap<>(),
      v -> null
    );

    mockServer.verify(1, anyRequestedFor(urlPathEqualTo("/endpoint")));
  }

  @Test
  public void testPostUriException() {
    assertThrows(
      IllegalArgumentException.class,
      () ->
        client.postOrPut(HttpPost::new, badUriParams, "", emptyMap, v -> null)
    );
  }

  @Test
  public void testPostRequestFailure() {
    assertThrows(
      UncheckedIOException.class,
      () ->
        client.postOrPut(
          HttpPost::new,
          badRequestParams,
          "",
          emptyMap,
          v -> null
        )
    );
  }

  @Test
  public void testPostRequestBodyFailure() {
    Object obj = new Object();
    assertThrows(
      IllegalArgumentException.class,
      () -> client.postOrPut(HttpPost::new, params, "endpoint", obj, v -> null)
    );
  }

  @Test
  public void testResponseEntityGetterSuccess() {
    JsonObject test = JsonObject.of("foo", "bar");

    // ensure appropriate headers are sent
    mockServer.stubFor(
      get(urlPathEqualTo("/endpoint")).willReturn(okJson(test.toString()))
    );

    assertThat(client.get(params, "endpoint", emptyMap).get(), is(test));
  }

  @Test
  public void testResponseEntityGetterBadCodeLow() {
    HttpResponse response = mock(HttpResponse.class);
    StatusLine status = mock(StatusLine.class);

    when(status.getStatusCode()).thenReturn(100);
    when(response.getStatusLine()).thenReturn(status);

    assertThat(ApiClientProxy.getResponseEntity(response).isEmpty(), is(true));
  }

  @Test
  public void testResponseEntityGetterBadCodeHigh() {
    HttpResponse response = mock(HttpResponse.class);
    StatusLine status = mock(StatusLine.class);

    when(status.getStatusCode()).thenReturn(400);
    when(response.getStatusLine()).thenReturn(status);

    assertThat(ApiClientProxy.getResponseEntity(response).isEmpty(), is(true));
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
    public Optional<JsonObject> get(
      OkapiConnectionParams params,
      String endpoint,
      Map<String, String> query
    ) {
      return super.get(params, endpoint, query);
    }

    @Override
    public Optional<JsonObject> postOrPut(
      Supplier<HttpEntityEnclosingRequestBase> createRequest,
      OkapiConnectionParams params,
      String endpoint,
      Object payload,
      Function<CloseableHttpResponse, Optional<JsonObject>> responseMapper
    ) {
      return super.postOrPut(
        createRequest,
        params,
        endpoint,
        payload,
        responseMapper
      );
    }
  }
}
