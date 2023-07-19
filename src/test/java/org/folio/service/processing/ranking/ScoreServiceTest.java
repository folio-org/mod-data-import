package org.folio.service.processing.ranking;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;

import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.util.Arrays;
import org.folio.dao.DataImportQueueItemDao;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.folio.rest.jaxrs.model.DataImportQueueItemCollection;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(VertxUnitRunner.class)
public class ScoreServiceTest {

  @Mock
  DataImportQueueItemDao queueItemDao;

  @Mock
  QueueItemHolisticRanker ranker;

  @InjectMocks
  ScoreService service;

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);
  }

  private DataImportQueueItem ofTenant(String tenant) {
    return new DataImportQueueItem().withTenant(tenant);
  }

  private DataImportQueueItemCollection collection(
    DataImportQueueItem... items
  ) {
    return new DataImportQueueItemCollection()
      .withDataImportQueueItems(Arrays.asList(items));
  }

  @Test
  public void testTenantUsageMap() {
    assertThat(ScoreService.getTenantUsageMap(collection()), is(anEmptyMap()));
    assertThat(
      ScoreService.getTenantUsageMap(collection(ofTenant("A"))),
      allOf(is(aMapWithSize(1)), hasEntry("A", 1L))
    );
    assertThat(
      ScoreService.getTenantUsageMap(
        collection(ofTenant("A"), ofTenant("A"), ofTenant("A"), ofTenant("A"))
      ),
      allOf(is(aMapWithSize(1)), hasEntry("A", 4L))
    );
    assertThat(
      ScoreService.getTenantUsageMap(
        collection(
          ofTenant("A"),
          ofTenant("B"),
          ofTenant("A"),
          ofTenant("A"),
          ofTenant("A")
        )
      ),
      allOf(is(aMapWithSize(2)), hasEntry("A", 4L), hasEntry("B", 1L))
    );
  }
}
