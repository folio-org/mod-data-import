package org.folio.service.processing.ranking;

import java.lang.reflect.Field;
import org.springframework.util.ReflectionUtils;

public abstract class AbstractQueueItemRankerTest {

  protected static final double EPSILON = 0.0000001;

  protected void setField(
    QueueItemRanker object,
    String fieldName,
    Object value
  ) {
    Field field = ReflectionUtils.findField(object.getClass(), fieldName);
    field.setAccessible(true);
    ReflectionUtils.setField(field, object, value);
  }
}
