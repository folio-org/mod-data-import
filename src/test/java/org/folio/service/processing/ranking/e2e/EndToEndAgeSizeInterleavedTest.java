package org.folio.service.processing.ranking.e2e;

import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.junit.runner.RunWith;

/**
 * Combines all rankers with realistic values and properties
 */
@RunWith(VertxUnitRunner.class)
public class EndToEndAgeSizeInterleavedTest
  extends AbstractEndToEndRankingTest {

  protected void initializeData() {
    DataImportQueueItem itemSize100Minutes0 = item("a", 100, 0, 1);
    DataImportQueueItem itemSize100Minutes5 = item("a", 100, 5, 1);
    DataImportQueueItem itemSize10000Minutes15 = item("a", 10000, 15, 1);
    DataImportQueueItem itemSize100Minutes60 = item("a", 100, 60, 1);
    DataImportQueueItem itemSize1000000Minutes4320 = item(
      "a",
      1000000,
      4320,
      1
    );

    // one waiting this long should exceed the extreme threshold
    // and jump the queue, despite being huge
    expected.add(itemSize1000000Minutes4320);
    expected.add(itemSize100Minutes60);
    expected.add(itemSize100Minutes5);
    // despite waiting longer, this one is de-prioritized as it is larger
    expected.add(itemSize10000Minutes15);
    expected.add(itemSize100Minutes0);

    waiting.add(itemSize100Minutes0);
    waiting.add(itemSize100Minutes5);
    waiting.add(itemSize10000Minutes15);
    waiting.add(itemSize100Minutes60);
    waiting.add(itemSize1000000Minutes4320);
  }
}
