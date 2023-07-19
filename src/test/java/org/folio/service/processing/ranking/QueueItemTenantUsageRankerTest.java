package org.folio.service.processing.ranking;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.is;

import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.util.Map;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class QueueItemTenantUsageRankerTest
  extends AbstractQueueItemRankerTest {

  private static final double EPSILON = 0.0000001;

  QueueItemTenantUsageRanker ranker;

  public QueueItemTenantUsageRankerTest() {
    this.ranker = new QueueItemTenantUsageRanker();

    this.setField(ranker, "scoreNoWorkers", 100);
    this.setField(ranker, "scoreAllWorkers", -100);
  }

  @Test
  public void testScoring() {
    DataImportQueueItem job = new DataImportQueueItem().withTenant("A");

    assertThat(ranker.score(job, Map.of()), is(closeTo(100, EPSILON)));
    assertThat(ranker.score(job, Map.of("A", 0L)), is(closeTo(100, EPSILON)));
    assertThat(ranker.score(job, Map.of("A", 1L)), is(closeTo(-100, EPSILON)));
    assertThat(ranker.score(job, Map.of("B", 2L)), is(closeTo(100, EPSILON)));
    assertThat(
      ranker.score(job, Map.of("A", 0L, "B", 2L)),
      is(closeTo(100, EPSILON))
    );
    assertThat(
      ranker.score(job, Map.of("A", 1L, "B", 3L)),
      is(closeTo(50, EPSILON))
    );
    assertThat(
      ranker.score(job, Map.of("A", 2L, "B", 2L)),
      is(closeTo(0, EPSILON))
    );
    assertThat(
      ranker.score(job, Map.of("A", 3L, "B", 1L)),
      is(closeTo(-50, EPSILON))
    );
  }
}
