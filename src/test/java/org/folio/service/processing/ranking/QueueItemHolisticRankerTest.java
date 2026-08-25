package org.folio.service.processing.ranking;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.within;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Map;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class QueueItemHolisticRankerTest extends AbstractQueueItemRankerTest {

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

  @DisplayName("should sum all ranker scores into holistic score")
  @Test
  void shouldSumAllRankerScores_whenScoring() {
    // arrange
    when(ageRanker.score(any(), any())).thenReturn(1d);
    when(partNumberRanker.score(any(), any())).thenReturn(2d);
    when(sizeRanker.score(any(), any())).thenReturn(3d);
    when(tenantUsageRanker.score(any(), any())).thenReturn(4d);

    DataImportQueueItem item = new DataImportQueueItem();
    Map<String, Long> tenantMap = Map.of();

    // act
    double result = ranker.score(item, tenantMap);

    // assert
    assertThat(result).isCloseTo(10, within(EPSILON));

    verify(ageRanker, times(1)).score(item, tenantMap);
    verify(partNumberRanker, times(1)).score(item, tenantMap);
    verify(sizeRanker, times(1)).score(item, tenantMap);
    verify(tenantUsageRanker, times(1)).score(item, tenantMap);

    verifyNoMoreInteractions(
      ageRanker,
      partNumberRanker,
      sizeRanker,
      tenantUsageRanker
    );
  }
}
