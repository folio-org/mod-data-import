package org.folio.service.processing.ranking;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.folio.rest.jaxrs.model.DataImportQueueItemCollection;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class ScoreServiceTenantUsageTest {

  @DisplayName("should build tenant usage map with correct counts per tenant")
  @Test
  void shouldBuildTenantUsageMap_whenCollectionHasVariousTenants() {
    assertThat(ScoreService.getTenantUsageMap(collectionOfTenant())).isEmpty();

    assertThat(ScoreService.getTenantUsageMap(collectionOfTenant("A")))
      .hasSize(1)
      .containsEntry("A", 1L);

    assertThat(ScoreService.getTenantUsageMap(collectionOfTenant("A", "A", "A", "A")))
      .hasSize(1)
      .containsEntry("A", 4L);

    assertThat(
      ScoreService.getTenantUsageMap(
        collectionOfTenant("A", "B", "A", "A", "A")
      )
    )
      .hasSize(2)
      .containsEntry("A", 4L)
      .containsEntry("B", 1L);
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

  private DataImportQueueItemCollection collectionOfTenant(String... items) {
    return collection(
      Arrays
        .stream(items)
        .map(this::ofTenant)
        .toArray(DataImportQueueItem[]::new)
    );
  }
}
