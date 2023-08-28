package org.folio.service.auth;

import static org.folio.dataimport.util.RestUtil.OKAPI_TENANT_HEADER;
import static org.folio.dataimport.util.RestUtil.OKAPI_TOKEN_HEADER;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.json.JsonObject;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import javax.ws.rs.core.MediaType;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dataimport.util.OkapiConnectionParams;

public abstract class ApiClient {

  private static final Logger LOGGER = LogManager.getLogger(ApiClient.class);

  private static ObjectMapper mapper = new ObjectMapper();

  protected Optional<JsonObject> get(
    OkapiConnectionParams params,
    String endpoint,
    Map<String, String> query
  ) {
    HttpGet request = new HttpGet();

    request.setHeader(OKAPI_TOKEN_HEADER, params.getToken());
    request.setHeader(OKAPI_TENANT_HEADER, params.getTenantId());
    request.setHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);
    request.setHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON);

    try {
      request.setURI(
        new URIBuilder(String.format("%s/%s", params.getOkapiUrl(), endpoint))
          .addParameters(
            query
              .entrySet()
              .stream()
              .map(e -> new BasicNameValuePair(e.getKey(), e.getValue()))
              .collect(Collectors.toList())
          )
          .build()
      );
    } catch (URISyntaxException e) {
      LOGGER.error("Invalid URL: ", e);
      throw new IllegalArgumentException(e);
    }

    LOGGER.info("Sending GET request {}", request);

    try (
      CloseableHttpResponse response = HttpClients
        .createDefault()
        .execute(request)
    ) {
      return getResponseEntity(response);
    } catch (IOException e) {
      LOGGER.error("Exception while calling {}", request.getURI(), e);
      throw new UncheckedIOException(e);
    }
  }

  protected Optional<JsonObject> post(
    OkapiConnectionParams params,
    String endpoint,
    Object payload
  ) {
    return postOrPut(() -> new HttpPost(), params, endpoint, payload);
  }

  protected Optional<JsonObject> put(
    OkapiConnectionParams params,
    String endpoint,
    Object payload
  ) {
    return postOrPut(() -> new HttpPut(), params, endpoint, payload);
  }

  protected Optional<JsonObject> postOrPut(
    Supplier<HttpEntityEnclosingRequestBase> createRequest,
    OkapiConnectionParams params,
    String endpoint,
    Object payload
  ) {
    HttpEntityEnclosingRequestBase request = createRequest.get();

    request.setHeader(OKAPI_TOKEN_HEADER, params.getToken());
    request.setHeader(OKAPI_TENANT_HEADER, params.getTenantId());
    request.setHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);
    request.setHeader(HttpHeaders.ACCEPT, MediaType.APPLICATION_JSON);

    try {
      request.setEntity(new StringEntity(mapper.writeValueAsString(payload)));
    } catch (UnsupportedEncodingException | JsonProcessingException e) {
      LOGGER.error("Invalid payload: ", e);
      throw new IllegalArgumentException(e);
    }

    try {
      request.setURI(
        new URI(String.format("%s/%s", params.getOkapiUrl(), endpoint))
      );
    } catch (URISyntaxException e) {
      LOGGER.error("Invalid URL: ", e);
      throw new IllegalArgumentException(e);
    }

    LOGGER.info("Sending POST request {} with payload {}", request, payload);

    try (
      CloseableHttpResponse response = HttpClients
        .createDefault()
        .execute(request)
    ) {
      return getResponseEntity(response);
    } catch (IOException e) {
      LOGGER.error("Exception while calling {}", request.getURI(), e);
      throw new UncheckedIOException(e);
    }
  }

  protected static Optional<JsonObject> getResponseEntity(
    CloseableHttpResponse response
  ) throws IOException {
    HttpEntity entity = response.getEntity();
    if (
      response.getStatusLine().getStatusCode() >= HttpStatus.SC_OK &&
      response.getStatusLine().getStatusCode() < HttpStatus.SC_BAD_REQUEST &&
      entity != null
    ) {
      String body = EntityUtils.toString(entity);
      LOGGER.debug("Response body: {}", body);
      return Optional.of(new JsonObject(body));
    } else {
      LOGGER.warn("Non-200 status code returned: {}", response.getStatusLine());
      return Optional.empty();
    }
  }
}
