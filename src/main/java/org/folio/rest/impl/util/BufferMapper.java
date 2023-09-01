package org.folio.rest.impl.util;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import lombok.experimental.UtilityClass;

/**
 * Util class which maps {@link Buffer} content to instance of specified type.
 */
@UtilityClass
public class BufferMapper {

  /**
   * Returns instantiated T entity object from a buffer content.
   *
   * @param buffer     buffer
   * @param entityType type of entity which will be created
   * @return a future of instantiated T entity object from a buffer content
   */
  public static <T> Future<T> mapBufferContentToEntity(
    Buffer buffer,
    Class<T> entityType
  ) {
    Promise<T> promise = Promise.promise();
    try {
      promise.complete(mapBufferContentToEntitySync(buffer, entityType));
    } catch (Exception e) {
      promise.fail(e);
    }
    return promise.future();
  }

  /**
   * Returns instantiated T entity object from a buffer content, synchronous.
   *
   * @param buffer     buffer
   * @param entityType type of entity which will be created
   * @return instantiated T entity object from a buffer content
   * @throws IllegalArgumentException if buffer content cannot be mapped to specified entity type
   */
  public static <T> T mapBufferContentToEntitySync(
    Buffer buffer,
    Class<T> entityType
  ) {
    return buffer.toJsonObject().mapTo(entityType);
  }
}