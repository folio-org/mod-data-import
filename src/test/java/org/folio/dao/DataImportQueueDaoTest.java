package org.folio.dao;


import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.UUID;

import org.folio.dao.util.PostgresClientFactory;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.folio.rest.persist.PostgresClient;
import org.folio.rest.persist.helpers.LocalRowSet;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;

import io.vertx.core.Future;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.sqlclient.Tuple;

@RunWith(VertxUnitRunner.class)
public class DataImportQueueDaoTest  {

  @Mock
  private PostgresClientFactory postgresClientFactory;

  @Mock
  private PostgresClient postgresClient;

  @InjectMocks
  DataImportQueueItemDao queueItemDaoImpl = new DataImportQueueItemDaoImpl();
  

  UUID storedItemUUID;
  @Before
  public void setUp() {
    MockitoAnnotations.initMocks(this);
    when(postgresClientFactory.getInstance()).thenReturn(postgresClient);
  }
  @Test 
  public void shouldAddQueueItem(TestContext context) {
    // given
    when(postgresClient.execute(anyString(), any(Tuple.class)))
      .thenReturn(Future.succeededFuture(new LocalRowSet(1)));
    // when
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
  queueItemDaoImpl.addQueueItem(queueItem)
    // then
    .onComplete(context.asyncAssertSuccess(x ->
      verify(postgresClient).execute(anyString(), any(Tuple.class))));
//    assert( queueItemDao.addQueueItem(queueItem).succeeded() == true);
  }
 
}
