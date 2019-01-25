package org.folio.service.processing.reader;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Testing MarcSourceReader
 */
@RunWith(MockitoJUnitRunner.class)
public class MarcSourceReaderUnitTest {

  private static final String SOURCE_PATH = "src/test/resources/CornellFOLIOExemplars.mrc";
  private static final int EXPECTED_RECORDS_NUMBER = 62;

  @Test
  public void shouldReturnAllRecords() {
    // given
    int chunkSize = 100;
    SourceReader reader = new MarcSourceReader(new File(SOURCE_PATH), chunkSize);
    List<String> actualRecords = new ArrayList<>();
    // when
    while (reader.hasNext()) {
      actualRecords.addAll(reader.next());
    }
    // then
    Assert.assertEquals(EXPECTED_RECORDS_NUMBER, actualRecords.size());
  }

  @Test
  public void shouldReturn2ChunksOfRecords() {
    // given
    int expectedChunksNumber = 2;
    int chunkSize = 31;
    SourceReader reader = new MarcSourceReader(new File(SOURCE_PATH), chunkSize);
    List<String> actualRecords = new ArrayList<>();
    int actualChunkNumber = 0;
    // when
    while (reader.hasNext()) {
      actualRecords.addAll(reader.next());
      actualChunkNumber++;
    }
    // then
    Assert.assertEquals(EXPECTED_RECORDS_NUMBER, actualRecords.size());
    Assert.assertEquals(expectedChunksNumber, actualChunkNumber);
  }

  @Test
  public void shouldReturn5ChunksOfRecords() {
    // given
    int expectedChunksNumber = 5;
    int chunkSize = 13;
    SourceReader reader = new MarcSourceReader(new File(SOURCE_PATH), chunkSize);
    List<String> actualRecords = new ArrayList<>();
    int actualChunkNumber = 0;
    // when
    while (reader.hasNext()) {
      actualRecords.addAll(reader.next());
      actualChunkNumber++;
    }
    // then
    Assert.assertEquals(EXPECTED_RECORDS_NUMBER, actualRecords.size());
    Assert.assertEquals(expectedChunksNumber, actualChunkNumber);
  }
}
