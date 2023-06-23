package org.folio.service;

import java.sql.Date;
import java.util.UUID;

import org.folio.dao.DataImportQueueItemDao;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.Criteria.Criterion;
import org.joda.time.DateTime;
import org.junit.Test;
import org.springframework.beans.factory.annotation.Autowired;

import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;

public class DataImportQueueTest extends AbstractIntegrationTest {

  private static String QUEUE_ITEM_TABLE = "queue_items";
  
  @Autowired
  DataImportQueueItemDao queueItemDao;
  UUID storedItemUUID;
  @Test
  public void shouldCreateQueueItem() {
    
    DataImportQueueItem queueItem = new DataImportQueueItem();
    queueItem.setFilePath("test/file/path");
    storedItemUUID = UUID.randomUUID();
    queueItem.setId(storedItemUUID.toString());
    queueItem.setUploadDefinitionId(UUID.randomUUID().toString());
    queueItem.setJobExecutionId(UUID.randomUUID().toString());
    queueItem.setSize(1000);
    queueItem.setOriginalSize(5000);
    DateTime now = new DateTime();
    queueItem.setTimestamp(now.toString());
   assert( queueItemDao.addQueueItem(queueItem).succeeded() == true);
  }

  @Override
  protected void clearTable(TestContext context) {
    Async async = context.async();
    PostgresClient.getInstance(vertx).delete(QUEUE_ITEM_TABLE, new Criterion(), event1 -> {
        if (event1.failed()) {
          context.fail(event1.cause());
        }
        async.complete();
      });
    
  }
}
