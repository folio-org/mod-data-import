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
 * Testing MarcJsonReader
 */
@RunWith(MockitoJUnitRunner.class)
public class MarcJsonReaderTest {
  private static final String SOURCE_PATH = "src/test/resources/ChalmersFOLIOExamples.json";
  private static final String INCORRECT_SOURCE_PATH = "src/test/resources/wrong.json";
  private static final String INCORRECT_TYPE_SOURCE_PATH = "src/test/resources/CornellFOLIOExemplars.mrc";
  private static final int EXPECTED_RECORDS_NUMBER = 62;
  private static final String MARC_TYPE = "MARC_JSON";

  @Test
  public void shouldReturnAllRecords() {
    //given
    int chunkSize = 100;
    SourceReader reader = new MarcJsonReader(new File(SOURCE_PATH), chunkSize);
    List<InitialRecord> actualRecords = new ArrayList<>();
    //when
    while (reader.hasNext()) {
      actualRecords.addAll(reader.next());
    }
    //then
    Assert.assertEquals(EXPECTED_RECORDS_NUMBER, actualRecords.size());
  }

  @Test
  public void shouldReturn4ChunksOfRecords() {
    //given
    int expectedChunksNumber = 4;
    int chunkSize = 16;
    SourceReader reader = new MarcJsonReader(new File(SOURCE_PATH), chunkSize);
    List<InitialRecord> actualRecords = new ArrayList<>();
    int actualChunkNumber = 0;
    //when
    while (reader.hasNext()) {
      actualRecords.addAll(reader.next());
      actualChunkNumber++;
    }
    //then
    Assert.assertEquals(EXPECTED_RECORDS_NUMBER, actualRecords.size());
    Assert.assertEquals(expectedChunksNumber, actualChunkNumber);
  }

  @Test
  public void getContentTypeShouldReturnMarcJsonTypeValue() {
    //given
    int chunkSize = 70;
    SourceReader reader = new MarcJsonReader(new File(SOURCE_PATH), chunkSize);
    //when
    String typeValue = reader.getContentType().toString();
    //then
    Assert.assertNotNull(typeValue);
    Assert.assertEquals(MARC_TYPE, typeValue);
  }

  @Test(expected = RecordsReaderException.class)
  public void marcJsonReaderConstructorShouldThrowRecordsReaderException() {
    //given
    int chunkSize = 50;
    //then
    new MarcJsonReader(new File(INCORRECT_SOURCE_PATH), chunkSize);
  }

  @Test(expected = RecordsReaderException.class)
  public void hasNextShouldThrowRecordsReaderException() {
    //given
    int chunkSize = 100;
    SourceReader reader = new MarcJsonReader(new File(INCORRECT_TYPE_SOURCE_PATH), chunkSize);
    //then
    reader.hasNext();
  }
}
