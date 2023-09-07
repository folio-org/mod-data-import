package org.folio.service.processing.ranking;

import io.vertx.core.Future;
import java.util.HashMap;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.dao.DataImportQueueItemDao;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.folio.rest.jaxrs.model.DataImportQueueItemCollection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ScoreService {

  private static final Logger LOGGER = LogManager.getLogger();

  private QueueItemHolisticRanker ranker;
  private DataImportQueueItemDao queueItemDao;

  @Autowired
  public ScoreService(
    QueueItemHolisticRanker ranker,
    DataImportQueueItemDao queueItemDao
  ) {
    this.ranker = ranker;
    this.queueItemDao = queueItemDao;
  }

  /**
   * Get the next best queue item that is currently waiting to execute and mark
   * it as in progress.
   * This is done <strong>as a single atomic action</strong>, meaning that only
   * one worker in a pool can execute this at a time, guaranteeing that no two
   * workers can get the same queue item
   */
  public Future<Optional<DataImportQueueItem>> getBestQueueItemAndMarkInProgress() {
    return queueItemDao.getAllQueueItemsAndProcessAtomic(
      (
        DataImportQueueItemCollection inProgress,
        DataImportQueueItemCollection waiting
      ) -> {
        NavigableSet<DataImportQueueItem> set = getRankedQueueItems(
          inProgress,
          waiting
        );
        if (set.isEmpty()) {
          return Optional.empty();
        }
        return Optional.of(set.first());
      }
    );
  }

  /**
   * Get a list of all WAITING queue items, sorted by score, with the highest
   * score coming first
   */
  public NavigableSet<DataImportQueueItem> getRankedQueueItems(
    DataImportQueueItemCollection inProgress,
    DataImportQueueItemCollection waiting
  ) {
    // we create a temporary map to store these, since the sorting comparator may
    // require multiple calls to `calculateScore`.
    // this is local to this method, since we don't want to cache these any longer
    // than one request/sorting.
    Map<DataImportQueueItem, Double> cache = new HashMap<>();

    Map<String, Long> tenantUsageMap = getTenantUsageMap(inProgress);
    LOGGER.info(
      "Calculating scores for {} waiting chunks",
      waiting.getDataImportQueueItems().size()
    );
    LOGGER.info("Current worker tenant usage: {}", tenantUsageMap);

    TreeSet<DataImportQueueItem> set = new TreeSet<>((a, b) -> {
      int scoreDiff = -Double.compare(
        calculateScoreCached(cache, a, tenantUsageMap),
        calculateScoreCached(cache, b, tenantUsageMap)
      );

      if (scoreDiff == 0) {
        // make deterministic
        return a.getId().compareTo(b.getId());
      }

      return scoreDiff;
    });

    set.addAll(waiting.getDataImportQueueItems());

    LOGGER.info("Calculated scores:");
    set
      .stream()
      .forEach(item ->
        LOGGER.info(
          "  {}/{}#{}: {}",
          item.getTenant(),
          item.getId(),
          item.getPartNumber(),
          calculateScoreCached(cache, item, tenantUsageMap)
        )
      );

    return set;
  }

  /**
   * Calculate the score for a single chunk
   */
  public double calculateScore(
    DataImportQueueItem item,
    Map<String, Long> tenantUsageMap
  ) {
    return ranker.score(item, tenantUsageMap);
  }

  /**
   * Creates a map of tenant -> number of chunks in progress
   */
  public static Map<String, Long> getTenantUsageMap(
    DataImportQueueItemCollection queueItems
  ) {
    return queueItems
      .getDataImportQueueItems()
      .stream()
      .collect(
        Collectors.groupingBy(
          DataImportQueueItem::getTenant,
          Collectors.counting()
        )
      );
  }

  private double calculateScoreCached(
    Map<DataImportQueueItem, Double> cache,
    DataImportQueueItem item,
    Map<String, Long> tenantUsageMap
  ) {
    return cache.computeIfAbsent(
      item,
      el -> calculateScore(item, tenantUsageMap)
    );
  }
}
