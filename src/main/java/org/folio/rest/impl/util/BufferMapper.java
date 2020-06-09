package org.folio.rest.impl.util;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;

/**
 * Util class which maps {@link Buffer} content to instance of specified type.
 */
public class BufferMapper {

  /**
   * Returns instantiated T entity object from a buffer content.
   *
   * @param buffer     buffer
   * @param entityType type of entity which will be created
   * @return instantiated T entity object from a buffer content
   */
  public static <T> Future<T> mapBufferContentToEntity(Buffer buffer, Class<T> entityType) {
    Promise<T> promise = Promise.promise();
    try {
      promise.complete(buffer.toJsonObject().mapTo(entityType));
    } catch (Exception e) {
      promise.fail(e);
    }
    return promise.future();
  }
}
