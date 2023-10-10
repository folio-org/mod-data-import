package org.folio.service.auth;

import static org.folio.dataimport.util.RestUtil.OKAPI_TENANT_HEADER;
import static org.folio.dataimport.util.RestUtil.OKAPI_TOKEN_HEADER;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.json.JsonObject;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import javax.ws.rs.core.MediaType;
import org.apache.http.HttpEntity;
import org.apache.http.HttpEntityEnclosingRequest;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.NameValuePair;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dataimport.util.OkapiConnectionParams;

public class ApiClient {

  private static final Logger LOGGER = LogManager.getLogger(ApiClient.class);

  private static ObjectMapper mapper = new ObjectMapper();

  protected <T> T sendRequest(
    HttpRequestBase request,
    OkapiConnectionParams params,
    String endpoint,
    Map<String, String> query,
    Function<HttpResponse, T> responseMapper
  ) {
    addOkapiHeaders(request, params);
    setUrl(request, params, endpoint, query);
    return dispatchRequest(request, responseMapper);
  }

  protected <T> T sendRequest(
    HttpEntityEnclosingRequestBase request,
    OkapiConnectionParams params,
    String endpoint,
    Map<String, String> query,
    Object payload,
    Function<HttpResponse, T> responseMapper
  ) {
    addOkapiHeaders(request, params);
    setUrl(request, params, endpoint, query);
    addPayload(request, payload);
    return dispatchRequest(request, responseMapper);
  }

  private static void addOkapiHeaders(
    HttpRequestBase request,
    OkapiConnectionParams params
  ) {
    request.setHeader(OKAPI_TOKEN_HEADER, params.getToken());
    request.setHeader(OKAPI_TENANT_HEADER, params.getTenantId());
  }

  private static void addPayload(
    HttpEntityEnclosingRequest request,
    Object payload
  ) {
    if (payload != null) {
      try {
        request.setHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);
        request.setEntity(new StringEntity(mapper.writeValueAsString(payload)));
      } catch (UnsupportedEncodingException | JsonProcessingException e) {
        LOGGER.error("Invalid payload: ", e);
        throw new IllegalArgumentException(e);
      }
    }
  }

  private static void setUrl(
    HttpRequestBase request,
    OkapiConnectionParams params,
    String endpoint,
    Map<String, String> queryParams
  ) {
    if (queryParams == null) {
      queryParams = Map.of();
    }
    try {
      request.setURI(
        new URIBuilder(String.format("%s/%s", params.getOkapiUrl(), endpoint))
          .addParameters(
            queryParams
              .entrySet()
              .stream()
              .map(e -> new BasicNameValuePair(e.getKey(), e.getValue()))
              .map(NameValuePair.class::cast)
              .toList()
          )
          .build()
      );
    } catch (URISyntaxException e) {
      LOGGER.error("Invalid URL: ", e);
      throw new IllegalArgumentException(e);
    }
  }

  private static <T> T dispatchRequest(
    HttpRequestBase request,
    Function<HttpResponse, T> responseMapper
  ) {
    LOGGER.debug("Sending request {}", request);

    try (
      CloseableHttpResponse response = HttpClients
        .createDefault()
        .execute(request)
    ) {
      return responseMapper.apply(response);
    } catch (IOException e) {
      LOGGER.error("Exception while calling {}", request.getURI(), e);
      throw new UncheckedIOException(e);
    }
  }

  protected static boolean isResponseOk(HttpResponse response) {
    return (
      response.getStatusLine().getStatusCode() >= HttpStatus.SC_OK &&
      response.getStatusLine().getStatusCode() < HttpStatus.SC_BAD_REQUEST
    );
  }

  protected static Optional<JsonObject> getResponseEntity(
    HttpResponse response
  ) {
    HttpEntity entity = response.getEntity();
    if (isResponseOk(response) && entity != null) {
      String body;

      try {
        body = EntityUtils.toString(entity);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }

      LOGGER.debug("Response body: {}", body);

      return Optional.of(new JsonObject(body));
    } else {
      LOGGER.warn("Non-2xx status code returned: {}", response.getStatusLine());
      return Optional.empty();
    }
  }
}
