package org.folio.service.processing.ranking;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import org.junit.Test;

public class ScoreUtilsExceptionalTest {

  // if only we had Lombok @UtilityClass
  // or assertThrows, for that matter...
  @Test(expected = InvocationTargetException.class)
  public void testInstantiation()
    throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
    Constructor<ScoreUtils> constructor =
      ScoreUtils.class.getDeclaredConstructor();
    constructor.setAccessible(true);
    constructor.newInstance();
  }
}
