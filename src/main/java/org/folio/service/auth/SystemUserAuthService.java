package org.folio.service.auth;

import io.vertx.core.Future;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dataimport.util.OkapiConnectionParams;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class SystemUserAuthService {

  private static final Logger LOGGER = LogManager.getLogger();

  private UsersClient usersClient;

  @Autowired
  public SystemUserAuthService(UsersClient usersClient) {
    this.usersClient = usersClient;
  }

  public Future<Void> prepareSystemUser(
    String tenantId,
    Map<String, String> headers
  ) {
    LOGGER.info(
      "PREPARING SYSTEM USER!!!! for tenantId={}, got headers {}",
      tenantId,
      headers
    );
    LOGGER.info(
      usersClient.getUserByUsername(
        new OkapiConnectionParams(headers, null),
        "diku_admin"
      )
    );
    return Future.succeededFuture();
  }
}
