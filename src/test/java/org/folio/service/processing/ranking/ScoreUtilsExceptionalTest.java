package org.folio.service.processing.ranking;

import static org.junit.Assert.assertThrows;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.junit.Test;

public class ScoreUtilsExceptionalTest {

  @Test
  public void testInstantiation()
      throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
    Constructor<ScoreUtils> constructor = ScoreUtils.class.getDeclaredConstructor();
    constructor.setAccessible(true);
    assertThrows(InvocationTargetException.class, () -> constructor.newInstance());
  }
}