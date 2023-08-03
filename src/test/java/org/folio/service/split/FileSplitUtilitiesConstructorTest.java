package org.folio.service.split;

import static org.junit.Assert.assertThrows;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import org.folio.service.processing.split.FileSplitUtilities;
import org.junit.Test;

public class FileSplitUtilitiesConstructorTest {

  @Test
  public void testInstantiation()
      throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
    Constructor<FileSplitUtilities> constructor = FileSplitUtilities.class.getDeclaredConstructor();
    constructor.setAccessible(true);
    assertThrows(InvocationTargetException.class, () -> constructor.newInstance());
  }
}
