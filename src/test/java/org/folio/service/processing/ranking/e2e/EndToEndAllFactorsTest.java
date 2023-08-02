package org.folio.service.processing.ranking.e2e;

import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.junit.runner.RunWith;

/**
 * Combines all rankers with realistic values and properties
 */
@RunWith(VertxUnitRunner.class)
public class EndToEndAllFactorsTest extends AbstractEndToEndRankingTest {

  protected void initializeData() {
    // new jobs from different tenants, otherwise equal
    DataImportQueueItem itemASize100Age0 = item("a", 100, 0, 1);
    DataImportQueueItem itemASize1000Age15Part1 = item("a", 1000, 15, 1);
    DataImportQueueItem itemASize1000Age15Part2 = item("a", 1000, 15, 2);
    DataImportQueueItem itemASize2000Age120 = item("a", 2000, 120, 2);

    DataImportQueueItem itemBSize100Age0 = item("b", 100, 0, 1);
    DataImportQueueItem itemBSize1000Age15Part1 = item("b", 1000, 15, 1);
    DataImportQueueItem itemBSize1000Age15Part2 = item("b", 1000, 15, 2);
    DataImportQueueItem itemBSize2000Age120 = item("b", 2000, 120, 2);

    DataImportQueueItem itemCSize100Age0 = item("c", 100, 0, 1);
    DataImportQueueItem itemCSize1000Age15Part1 = item("c", 1000, 15, 1);
    DataImportQueueItem itemCSize1000Age15Part2 = item("c", 1000, 15, 2);
    DataImportQueueItem itemCSize2000Age120 = item("c", 2000, 120, 2);

    DataImportQueueItem itemDSize100Age0 = item("d", 100, 0, 1);
    DataImportQueueItem itemDSize1000Age15Part1 = item("d", 1000, 15, 1);
    DataImportQueueItem itemDSize1000Age15Part2 = item("d", 1000, 15, 2);
    DataImportQueueItem itemDSize2000Age120 = item("d", 2000, 120, 2);

    // A is using 3/6, B 2/6, and C 1/6
    // these measures do not rely on the job, just tenant name, so we can recycle
    inProgress.add(itemASize100Age0);
    inProgress.add(itemASize100Age0);
    inProgress.add(itemASize100Age0);
    inProgress.add(itemBSize100Age0);
    inProgress.add(itemBSize100Age0);
    inProgress.add(itemCSize100Age0);

    // D has priority tenant-wise, but C's old job can squeeze through
    // newer jobs are somewhat bumped down over older jobs, e.g. B(age=0) below A(age=120)
    // despite it having a better size and tenant usage
    expected.add(itemDSize2000Age120);
    expected.add(itemDSize1000Age15Part1);
    expected.add(itemDSize1000Age15Part2);
    expected.add(itemCSize2000Age120);
    expected.add(itemDSize100Age0);
    expected.add(itemCSize1000Age15Part1);
    expected.add(itemCSize1000Age15Part2);
    expected.add(itemBSize2000Age120);
    expected.add(itemCSize100Age0);
    expected.add(itemBSize1000Age15Part1);
    expected.add(itemBSize1000Age15Part2);
    expected.add(itemASize2000Age120);
    expected.add(itemBSize100Age0);
    expected.add(itemASize1000Age15Part1);
    expected.add(itemASize1000Age15Part2);
    expected.add(itemASize100Age0);

    waiting.add(itemASize100Age0);
    waiting.add(itemASize1000Age15Part1);
    waiting.add(itemASize1000Age15Part2);
    waiting.add(itemASize2000Age120);
    waiting.add(itemBSize100Age0);
    waiting.add(itemBSize1000Age15Part1);
    waiting.add(itemBSize1000Age15Part2);
    waiting.add(itemBSize2000Age120);
    waiting.add(itemCSize100Age0);
    waiting.add(itemCSize1000Age15Part1);
    waiting.add(itemCSize1000Age15Part2);
    waiting.add(itemCSize2000Age120);
    waiting.add(itemDSize100Age0);
    waiting.add(itemDSize1000Age15Part2);
    waiting.add(itemDSize1000Age15Part1);
    waiting.add(itemDSize2000Age120);
  }
}
