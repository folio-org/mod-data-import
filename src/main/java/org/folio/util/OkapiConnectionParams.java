package org.folio.util;

import io.vertx.core.Vertx;

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

  public OkapiConnectionParams(String okapiUrl, String tenantId, String token, Vertx vertx, Integer timeout) {
    this.okapiUrl = okapiUrl;
    this.tenantId = tenantId;
    this.token = token;
    this.vertx = vertx;
    this.timeout = timeout;
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
    return timeout != null ? timeout : DEF_TIMEOUT;
  }
}
