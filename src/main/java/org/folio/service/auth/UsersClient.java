package org.folio.service.auth;

import static org.folio.dataimport.util.RestUtil.OKAPI_TENANT_HEADER;
import static org.folio.dataimport.util.RestUtil.OKAPI_TOKEN_HEADER;

import io.vertx.core.json.JsonObject;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URISyntaxException;
import java.util.Map;
import java.util.stream.Collectors;
import javax.ws.rs.core.MediaType;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.springframework.stereotype.Component;

@Component
public class UsersClient {

  private static final Logger LOGGER = LogManager.getLogger(UsersClient.class);

  private static final String GET_USERS_BY_USERNAME = "users";
  private static final String GET_USERS_BY_USERNAME_QUERY = "username=\"%s\"";

  public JsonObject getUserByUsername(
    OkapiConnectionParams params,
    String username
  ) {
    return get(
      params,
      GET_USERS_BY_USERNAME,
      Map.of("query", String.format(GET_USERS_BY_USERNAME_QUERY, username))
    );
  }

  private JsonObject get(
    OkapiConnectionParams params,
    String endpoint,
    Map<String, String> query
  ) {
    HttpGet httpGet = new HttpGet();

    httpGet.setHeader(OKAPI_TOKEN_HEADER, params.getToken());
    httpGet.setHeader(OKAPI_TENANT_HEADER, params.getTenantId());
    httpGet.setHeader(HttpHeaders.CONTENT_TYPE, MediaType.APPLICATION_JSON);
    httpGet.setHeader((HttpHeaders.ACCEPT), MediaType.APPLICATION_JSON);
    try {
      httpGet.setURI(
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

    LOGGER.info("Sending to {} request {}", params.getOkapiUrl(), httpGet);

    try (
      CloseableHttpResponse response = HttpClients
        .createDefault()
        .execute(httpGet)
    ) {
      return getResponseEntity(response);
    } catch (IOException e) {
      LOGGER.error("Exception while calling {}", httpGet.getURI(), e);
      throw new UncheckedIOException(e);
    }
  }

  protected static JsonObject getResponseEntity(CloseableHttpResponse response)
    throws IOException {
    HttpEntity entity = response.getEntity();
    if (
      response.getStatusLine().getStatusCode() == HttpStatus.SC_OK &&
      entity != null
    ) {
      String body = EntityUtils.toString(entity);
      LOGGER.debug("Response body: {}", body);
      return new JsonObject(body);
    } else {
      throw new IOException(
        "Get invalid response with status: " +
        response.getStatusLine().getStatusCode()
      );
    }
  }
}
