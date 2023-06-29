package org.folio.service.processing.ranking;

import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.lang.reflect.Field;
import org.junit.runner.RunWith;
import org.springframework.util.ReflectionUtils;

@RunWith(VertxUnitRunner.class)
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
