package org.folio.service.processing.reader;

import org.folio.rest.jaxrs.model.InitialRecord;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Testing MarcXmlReader
 */
@RunWith(MockitoJUnitRunner.class)
public class MarcXmlReaderTest {
  private static final String SOURCE_PATH = "src/test/resources/UChicago_SampleBibs.xml";
  private static final String INCORRECT_SOURCE_PATH = "src/test/resources/wrong.json";
  private static final int EXPECTED_RECORDS_NUMBER = 62;
  private static final String MARC_TYPE = "MARC_XML";

  @Test
  public void shouldReturnAllRecordsFromXmlFile() {
    //given
    int chunkSize = 100;
    SourceReader reader = new MarcXmlReader(new File(SOURCE_PATH), chunkSize);
    List<InitialRecord> actualRecords = new ArrayList<>();
    //when
    while (reader.hasNext()) {
      actualRecords.addAll(reader.next());
    }
    //then
    Assert.assertEquals(EXPECTED_RECORDS_NUMBER, actualRecords.size());
  }

  @Test
  public void getContentTypeShouldReturnMarcXmlTypeValue() {
    //given
    int chunkSize = 50;
    SourceReader reader = new MarcXmlReader(new File(SOURCE_PATH), chunkSize);
    //when
    String typeValue = reader.getContentType().toString();
    //then
    Assert.assertNotNull(typeValue);
    Assert.assertEquals(MARC_TYPE, typeValue);
  }

  @Test(expected = RecordsReaderException.class)
  public void marcXmlReaderConstructorShouldThrowRecordsReaderException() {
    //given
    int chunkSize = 40;
    //then
    new MarcXmlReader(new File(INCORRECT_SOURCE_PATH), chunkSize);
  }
}
