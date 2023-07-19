package org.folio.service.processing.ranking;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.util.Map;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(VertxUnitRunner.class)
public class QueueItemHolisticRankerTest extends AbstractQueueItemRankerTest {

  @Mock
  QueueItemAgeRanker ageRanker;

  @Mock
  QueueItemPartNumberRanker partNumberRanker;

  @Mock
  QueueItemSizeRanker sizeRanker;

  @Mock
  QueueItemTenantUsageRanker tenantUsageRanker;

  @InjectMocks
  QueueItemHolisticRanker ranker;

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  public void testScoring() {
    when(ageRanker.score(any(), any())).thenReturn(1d);
    when(partNumberRanker.score(any(), any())).thenReturn(2d);
    when(sizeRanker.score(any(), any())).thenReturn(3d);
    when(tenantUsageRanker.score(any(), any())).thenReturn(4d);

    DataImportQueueItem item = new DataImportQueueItem();
    Map<String, Long> tenantMap = Map.of();

    assertThat(ranker.score(item, tenantMap), is(closeTo(10, EPSILON)));

    verify(ageRanker, times(1)).score(item, tenantMap);
    verify(partNumberRanker, times(1)).score(item, tenantMap);
    verify(sizeRanker, times(1)).score(item, tenantMap);
    verify(tenantUsageRanker, times(1)).score(item, tenantMap);
  }
}
