package org.folio.service.processing.ranking.e2e;

import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.junit.runner.RunWith;

/**
 * Combines all rankers with realistic values and properties
 */
@RunWith(VertxUnitRunner.class)
public class EndToEndTenantUsageTest extends AbstractEndToEndRankingTest {

  protected void initializeData() {
    // new jobs from different tenants, otherwise equal
    DataImportQueueItem itemA = item("a", 100, 0, 1);
    DataImportQueueItem itemB = item("b", 100, 0, 1);
    DataImportQueueItem itemC = item("c", 100, 0, 1);
    DataImportQueueItem itemD = item("d", 100, 0, 1);

    // A is using 3/6, B 2/6, and C 1/6
    inProgress.add(itemA);
    inProgress.add(itemA);
    inProgress.add(itemA);
    inProgress.add(itemB);
    inProgress.add(itemB);
    inProgress.add(itemC);

    // tenants using less workers should be prioritized
    expected.add(itemD);
    expected.add(itemC);
    expected.add(itemB);
    expected.add(itemA);

    waiting.add(itemA);
    waiting.add(itemB);
    waiting.add(itemC);
    waiting.add(itemD);
  }
}
