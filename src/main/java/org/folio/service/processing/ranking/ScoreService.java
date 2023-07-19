package org.folio.service.processing.ranking;

import io.vertx.core.CompositeFuture;
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

  @Autowired
  private QueueItemHolisticRanker ranker;

  @Autowired
  private DataImportQueueItemDao queueItemDao;

  /**
   * Get the next best queue item that is currently waiting to execute
   */
  public Future<Optional<DataImportQueueItem>> getBestQueueItem() {
    return this.getRankedQueueItems()
      .map(set -> Optional.ofNullable(set.first()));
  }

  /**
   * Get a list of all WAITING queue items, sorted by score, with the highest
   * score coming first
   */
  public Future<NavigableSet<DataImportQueueItem>> getRankedQueueItems() {
    // we create a temporary map to store these, since the sorting comparator may
    // require multiple calls to `calculateScore`.
    // this is local to this method, since we don't want to cache these any longer
    // than one request/sorting.
    Map<DataImportQueueItem, Double> cache = new HashMap<>();

    return CompositeFuture
      .all(
        queueItemDao.getAllInProgressQueueItems(),
        queueItemDao.getAllWaitingQueueItems()
      )
      .map((CompositeFuture result) -> {
        DataImportQueueItemCollection inProgress = result.resultAt(0);
        DataImportQueueItemCollection waiting = result.resultAt(1);

        Map<String, Long> tenantUsageMap = getTenantUsageMap(inProgress);
        LOGGER.info(
          "Calculating scores for {} waiting chunks",
          waiting.getDataImportQueueItems().size()
        );
        LOGGER.info(
          "Current worker tenant usage (tenant(number of workers)): {}",
          tenantUsageMap
            .entrySet()
            .stream()
            .map(e -> String.format("%s(%d)", e.getKey(), e.getValue()))
            .collect(Collectors.joining(", "))
        );

        TreeSet<DataImportQueueItem> set = new TreeSet<>((a, b) ->
          -Double.compare(
            calculateScoreCached(cache, a, tenantUsageMap),
            calculateScoreCached(cache, b, tenantUsageMap)
          )
        );

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

        return null;
      });
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
  public Map<String, Long> getTenantUsageMap(
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
