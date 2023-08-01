package org.folio.service.processing.ranking.e2e;

import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.runner.RunWith;

/**
 * Combines all rankers with realistic values and properties
 */
@RunWith(VertxUnitRunner.class)
public class EndToEndSingleJobPartsTest extends AbstractEndToEndRankingTest {

  protected void initializeData() {
    for (int i = 1; i <= 20; i++) {
      expected.add(item("a", 1000, 0, i));
    }

    // testing in reverse order
    for (int i = 19; i >= 0; i--) {
      waiting.add(expected.get(i));
    }
  }
}
