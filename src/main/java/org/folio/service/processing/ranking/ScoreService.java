package org.folio.service.processing.ranking;

import org.folio.rest.jaxrs.model.DataImportQueueItem;

public interface ScoreService {
  /**
   * Calculate the score for a given chunk based on the class's metric/implementation
   */
  double calculateScore(DataImportQueueItem chunk);
}
