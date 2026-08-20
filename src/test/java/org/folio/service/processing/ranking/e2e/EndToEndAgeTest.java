package org.folio.service.processing.ranking.e2e;

/**
 * Combines all rankers with realistic values and properties.
 */
public class EndToEndAgeTest extends AbstractEndToEndRankingTest {

  protected void initializeData() {
    final var itemMinutes0 = item("a", 1000, 0, 1);
    final var itemMinutes5 = item("a", 1000, 5, 1);
    final var itemMinutes15 = item("a", 1000, 15, 1);
    final var itemMinutes60 = item("a", 1000, 60, 1);
    final var itemMinutes4320 = item("a", 1000, 4320, 1);

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
