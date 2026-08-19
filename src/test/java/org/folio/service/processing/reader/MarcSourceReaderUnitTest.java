package org.folio.service.processing.reader;

import org.folio.rest.jaxrs.model.InitialRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
class MarcSourceReaderUnitTest {

  private static final String SOURCE_PATH = "src/test/resources/CornellFOLIOExemplars.mrc";
  private static final String SOURCE_WITH_WRONG_ENCODING_PATH = "src/test/resources/61PRINT160129.mrc";
  private static final int EXPECTED_RECORDS_NUMBER = 62;
  private static final int EXPECTED_RECORDS_NUMBER_CHUNK_SIZE_100 = 246;
  private static final String MARC_TYPE = "MARC_RAW";

  @DisplayName("should return all records from a standard MARC RAW file")
  @Test
  void shouldReturnAllRecords() {
    // arrange
    int chunkSize = 100;
    SourceReader reader = new MarcRawReader(new File(SOURCE_PATH), chunkSize);
    List<InitialRecord> actualRecords = new ArrayList<>();

    // act
    while (reader.hasNext()) {
      actualRecords.addAll(reader.next());
    }

    // assert
    assertThat(actualRecords).hasSize(EXPECTED_RECORDS_NUMBER);
  }

  @DisplayName("should return all records from a MARC RAW file with non-standard encoding")
  @Test
  void shouldReturnAllRecords_whenFileHasWrongEncoding() {
    // arrange
    int chunkSize = 100;
    SourceReader reader = new MarcRawReader(new File(SOURCE_WITH_WRONG_ENCODING_PATH), chunkSize);
    List<InitialRecord> actualRecords = new ArrayList<>();

    // act
    while (reader.hasNext()) {
      actualRecords.addAll(reader.next());
    }

    // assert
    assertThat(actualRecords).hasSize(EXPECTED_RECORDS_NUMBER_CHUNK_SIZE_100);
  }

  @DisplayName("should return 2 chunks when chunk size is 31")
  @Test
  void shouldReturn2ChunksOfRecords() {
    // arrange
    int expectedChunksNumber = 2;
    int chunkSize = 31;
    SourceReader reader = new MarcRawReader(new File(SOURCE_PATH), chunkSize);
    List<InitialRecord> actualRecords = new ArrayList<>();
    int actualChunkNumber = 0;

    // act
    while (reader.hasNext()) {
      actualRecords.addAll(reader.next());
      actualChunkNumber++;
    }

    // assert
    assertThat(actualRecords).hasSize(EXPECTED_RECORDS_NUMBER);
    assertThat(actualChunkNumber).isEqualTo(expectedChunksNumber);
  }

  @DisplayName("should return 5 chunks when chunk size is 13")
  @Test
  void shouldReturn5ChunksOfRecords() {
    // arrange
    int expectedChunksNumber = 5;
    int chunkSize = 13;
    SourceReader reader = new MarcRawReader(new File(SOURCE_PATH), chunkSize);
    List<InitialRecord> actualRecords = new ArrayList<>();
    int actualChunkNumber = 0;

    // act
    while (reader.hasNext()) {
      actualRecords.addAll(reader.next());
      actualChunkNumber++;
    }

    // assert
    assertThat(actualRecords).hasSize(EXPECTED_RECORDS_NUMBER);
    assertThat(actualChunkNumber).isEqualTo(expectedChunksNumber);
  }

  @DisplayName("should return MARC_RAW as content type")
  @Test
  void shouldReturnMarcRawContentType() {
    // arrange
    int chunkSize = 77;
    SourceReader reader = new MarcRawReader(new File(SOURCE_PATH), chunkSize);

    // act
    String typeValue = reader.getContentType().toString();

    // assert
    assertThat(typeValue).isNotNull().isEqualTo(MARC_TYPE);
  }
}
