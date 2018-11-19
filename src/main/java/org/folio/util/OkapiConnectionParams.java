package org.folio.util;

import io.vertx.core.Vertx;

import java.util.Map;

import static org.folio.util.RestUtil.OKAPI_TENANT_HEADER;
import static org.folio.util.RestUtil.OKAPI_TOKEN_HEADER;
import static org.folio.util.RestUtil.OKAPI_URL_HEADER;

/**
 * Wrapper class for Okapi connection params
 */
public class OkapiConnectionParams {

  private static final int DEF_TIMEOUT = 2000;
  private String okapiUrl;
  private String tenantId;
  private String token;
  private Vertx vertx;
  private Integer timeout;

  public OkapiConnectionParams(Map<String, String> okapiHeaders, Vertx vertx, Integer timeout) {
    this.okapiUrl = okapiHeaders.getOrDefault(OKAPI_URL_HEADER, "localhost");
    this.tenantId = okapiHeaders.getOrDefault(OKAPI_TENANT_HEADER, "");
    this.token = okapiHeaders.getOrDefault(OKAPI_TOKEN_HEADER, "dummy");
    this.vertx = vertx;
    this.timeout = timeout != null ? timeout : DEF_TIMEOUT;
  }

  public OkapiConnectionParams(Map<String, String> okapiHeaders, Vertx vertx) {
    this.okapiUrl = okapiHeaders.getOrDefault(OKAPI_URL_HEADER, "localhost");
    this.tenantId = okapiHeaders.getOrDefault(OKAPI_TENANT_HEADER, "");
    this.token = okapiHeaders.getOrDefault(OKAPI_TOKEN_HEADER, "dummy");
    this.vertx = vertx;
    this.timeout = DEF_TIMEOUT;
  }

  public String getOkapiUrl() {
    return okapiUrl;
  }

  public String getTenantId() {
    return tenantId;
  }

  public String getToken() {
    return token;
  }

  public Vertx getVertx() {
    return vertx;
  }

  public int getTimeout() {
    return timeout;
  }
}
