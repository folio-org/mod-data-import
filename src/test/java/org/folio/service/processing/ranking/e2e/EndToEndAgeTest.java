package org.folio.service.processing.ranking.e2e;

import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.junit.runner.RunWith;

/**
 * Combines all rankers with realistic values and properties
 */
@RunWith(VertxUnitRunner.class)
public class EndToEndAgeTest extends AbstractEndToEndRankingTest {

  protected void initializeData() {
    DataImportQueueItem itemMinutes0 = item("a", 1000, 0, 1);
    DataImportQueueItem itemMinutes5 = item("a", 1000, 5, 1);
    DataImportQueueItem itemMinutes15 = item("a", 1000, 15, 1);
    DataImportQueueItem itemMinutes60 = item("a", 1000, 60, 1);
    DataImportQueueItem itemMinutes4320 = item("a", 1000, 4320, 1);

    expected.add(itemMinutes4320);
    expected.add(itemMinutes60);
    expected.add(itemMinutes15);
    expected.add(itemMinutes5);
    expected.add(itemMinutes0);

    waiting.add(itemMinutes60);
    waiting.add(itemMinutes0);
    waiting.add(itemMinutes15);
    waiting.add(itemMinutes4320);
    waiting.add(itemMinutes5);
  }
}
