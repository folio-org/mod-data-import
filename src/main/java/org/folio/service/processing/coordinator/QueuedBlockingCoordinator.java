package org.folio.service.processing.coordinator;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Implementation of the BlockingCoordinator based on bounded blocking queue.
 * Using bounded queue is a way to design concurrent programs because when we insert an element
 * to an already full queue, that operations need to wait until consumers catch up and make some space available
 * in the queue. It gives a throughput control.
 */
public class QueuedBlockingCoordinator implements BlockingCoordinator {

  private static final Object QUEUE_ITEM = new Object();
  private BlockingQueue<Object> blockingQueue = null;

  public QueuedBlockingCoordinator(int capacity) {
    blockingQueue = new ArrayBlockingQueue<>(capacity, true);
  }

  public void acceptLock() {
    try {
      blockingQueue.put(QUEUE_ITEM);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public void acceptUnlock() {
    try {
      if (!blockingQueue.isEmpty()) {
        blockingQueue.take();
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}
