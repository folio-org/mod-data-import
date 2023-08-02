package org.folio.service.processing.ranking;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.aMapWithSize;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;

import java.util.Arrays;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.folio.rest.jaxrs.model.DataImportQueueItemCollection;
import org.junit.Test;

public class ScoreServiceTenantUsageTest {

  private DataImportQueueItem ofTenant(String tenant) {
    return new DataImportQueueItem().withTenant(tenant);
  }

  private DataImportQueueItemCollection collection(
    DataImportQueueItem... items
  ) {
    return new DataImportQueueItemCollection()
      .withDataImportQueueItems(Arrays.asList(items));
  }

  private DataImportQueueItemCollection collectionOfTenant(String... items) {
    return collection(
      Arrays
        .stream(items)
        .map(this::ofTenant)
        .toArray(DataImportQueueItem[]::new)
    );
  }

  @Test
  public void testTenantUsageMap() {
    assertThat(
      ScoreService.getTenantUsageMap(collectionOfTenant()),
      is(anEmptyMap())
    );
    assertThat(
      ScoreService.getTenantUsageMap(collectionOfTenant("A")),
      allOf(is(aMapWithSize(1)), hasEntry("A", 1L))
    );
    assertThat(
      ScoreService.getTenantUsageMap(collectionOfTenant("A", "A", "A", "A")),
      allOf(is(aMapWithSize(1)), hasEntry("A", 4L))
    );
    assertThat(
      ScoreService.getTenantUsageMap(
        collectionOfTenant("A", "B", "A", "A", "A")
      ),
      allOf(is(aMapWithSize(2)), hasEntry("A", 4L), hasEntry("B", 1L))
    );
  }
}
