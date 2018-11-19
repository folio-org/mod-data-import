package org.folio.util;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.folio.rest.client.ConfigurationsClient;

import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.folio.util.RestUtil.OKAPI_TENANT_HEADER;
import static org.folio.util.RestUtil.OKAPI_TOKEN_HEADER;
import static org.folio.util.RestUtil.OKAPI_URL_HEADER;

/**
 * Util for loading configuration values from mod-configuration
 */
public class ConfigurationUtil {

  private static final String MODULE_CODE = "DATA_IMPORT";
  private static final Pattern HOST_PORT_PATTERN = Pattern.compile("https?://([^:/]+)(?::?(\\d+)?)");
  private static final int DEFAULT_PORT = 9030;

  private ConfigurationUtil() {
  }

  /**
   * Load property value from mod-config by code
   *
   * @param code - property code
   * @return a list of user fields to use for search
   */
  public static Future<String> getPropertyByCode(String code, Map<String, String> okapiHeaders) {
    Future<String> future = Future.future();
    String okapiURL = okapiHeaders.get(OKAPI_URL_HEADER);
    String tenant = okapiHeaders.get(OKAPI_TENANT_HEADER);
    String token = okapiHeaders.get(OKAPI_TOKEN_HEADER);
    try {
      Matcher matcher = HOST_PORT_PATTERN.matcher(okapiURL);
      if (!matcher.find()) {
        future.fail("Could not parse okapiURL: " + okapiURL);
        return future;
      }

      String host = matcher.group(1);
      String port = matcher.group(2);

      ConfigurationsClient configurationsClient = new ConfigurationsClient(host, StringUtils.isNotBlank(port) ? Integer.valueOf(port) : DEFAULT_PORT, tenant, token);
      StringBuilder query = new StringBuilder("module==")
        .append(MODULE_CODE)
        .append(" AND ( code==\"")
        .append(code)
        .append("\")");
      configurationsClient.getEntries(query.toString(), 0, 3, null, null, response ->
        response.bodyHandler(body -> {
          if (response.statusCode() != 200) {
            future.fail("Expected status code 200, got '" + response.statusCode() +
              "' :" + body.toString());
            return;
          }
          JsonObject entries = body.toJsonObject();
          Integer total = entries.getInteger("totalRecords");
          if (total != null && total > 0) {
            future.complete(
              entries.getJsonArray("configs")
                .getJsonObject(0)
                .getString("value"));
          } else {
            future.fail("No config values was found");
          }
        })
      );
    } catch (Exception e) {
      future.fail(e);
    }
    return future;
  }

}
