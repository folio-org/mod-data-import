package org.folio.service.processing.reader;

import org.folio.rest.jaxrs.model.InitialRecord;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

@RunWith(MockitoJUnitRunner.class)
public class EdifactReaderUnitTest {

  private static final String SOURCE_PATH_FR = "src/test/resources/CornAuxAm.1605541205.edi";
  private static final String SOURCE_PATH_IT = "src/test/resources/CornCasalini.1606151339.edi";
  private static final String SOURCE_PATH_US_UK = "src/test/resources/A-MGOBIe-orders565750us20200903.edi";

  @Test
  public void shouldReturnAllRecords1() {
    // given
    SourceReader reader = new EdifactReader(new File(SOURCE_PATH_FR));
    List<InitialRecord> actualRecords = new ArrayList<>();
    // when
    actualRecords.addAll(reader.next());
    // then
    Assert.assertEquals(1, actualRecords.size());
  }

  @Test
  public void shouldReturnAllRecords2() {
    // given
    SourceReader reader = new EdifactReader(new File(SOURCE_PATH_IT));
    List<InitialRecord> actualRecords = new ArrayList<>();
    // when
    actualRecords.addAll(reader.next());
    // then
    Assert.assertEquals(1, actualRecords.size());
  }

  @Test
  public void shouldReturnAllRecords3() {
    // given
    SourceReader reader = new EdifactReader(new File(SOURCE_PATH_US_UK));
    List<InitialRecord> actualRecords = new ArrayList<>();
    // when
    actualRecords.addAll(reader.next());
    // then
    Assert.assertEquals(3, actualRecords.size());
  }

}
