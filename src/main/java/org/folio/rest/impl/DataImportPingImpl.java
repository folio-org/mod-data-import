package org.folio.rest.impl;

import io.vertx.core.AsyncResult;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;
import io.vertx.core.shareddata.LocalMap;
import io.vertx.core.shareddata.SharedData;
import org.folio.rest.jaxrs.model.PingMessage;
import org.folio.rest.jaxrs.model.PingMessageCollection;
import org.folio.rest.jaxrs.resource.DataImportPing;

import javax.ws.rs.core.Response;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class DataImportPingImpl implements DataImportPing {

  @Override
  public void postDataImportPing(String lang,
                                 PingMessage entity,
                                 Map<String, String> okapiHeaders,
                                 Handler<AsyncResult<Response>> asyncResultHandler,
                                 Context vertxContext) {
    //TODO replace stub response
    entity.setId(UUID.randomUUID().toString());
    asyncResultHandler.handle(Future.succeededFuture(
      PostDataImportPingResponse.respond201WithApplicationJson(
        entity,
        PostDataImportPingResponse.headersFor201().withLocation("/data-import-ping/" + entity.getId())
      )));
  }

  @Override
  public void getDataImportPing(String query,
                                int offset,
                                int limit,
                                String lang,
                                Map<String, String> okapiHeaders,
                                Handler<AsyncResult<Response>> asyncResultHandler,
                                Context vertxContext) {
    //TODO replace stub response
    SharedData sharedData = vertxContext.owner().sharedData();
    LocalMap<String, JsonObject> pingMessages = sharedData.getLocalMap("pingMessages");
    List<PingMessage> messagesList = pingMessages.entrySet().stream()
      .map(stringJsonObjectEntry -> stringJsonObjectEntry.getValue().mapTo(PingMessage.class))
      .collect(Collectors.toList());

    asyncResultHandler.handle(Future.succeededFuture(
      GetDataImportPingResponse.respond200WithApplicationJson(
        new PingMessageCollection()
          .withPingMessages(messagesList)
          .withTotalRecords(messagesList.size()))
    ));
  }
}
