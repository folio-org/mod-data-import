package org.folio.service.processing.reader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import org.folio.rest.jaxrs.model.InitialRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class MarcJsonReaderTest {
  private static final String SOURCE_PATH = "src/test/resources/ChalmersFOLIOExamples.json";
  private static final String INCORRECT_SOURCE_PATH = "src/test/resources/wrong.json";
  private static final String INCORRECT_TYPE_SOURCE_PATH = "src/test/resources/CornellFOLIOExemplars.mrc";
  private static final int EXPECTED_RECORDS_NUMBER = 62;
  private static final String MARC_TYPE = "MARC_JSON";

  @DisplayName("should return all records from a MARC JSON file in a single chunk")
  @Test
  void shouldReturnAllRecords() {
    // arrange
    int chunkSize = 100;
    SourceReader reader = new MarcJsonReader(new File(SOURCE_PATH), chunkSize);
    List<InitialRecord> actualRecords = new ArrayList<>();

    // act
    while (reader.hasNext()) {
      actualRecords.addAll(reader.next());
    }

    // assert
    assertThat(actualRecords).hasSize(EXPECTED_RECORDS_NUMBER);
  }

  @DisplayName("should return 4 chunks when chunk size is 16")
  @Test
  void shouldReturn4ChunksOfRecords() {
    // arrange
    int expectedChunksNumber = 4;
    int chunkSize = 16;
    SourceReader reader = new MarcJsonReader(new File(SOURCE_PATH), chunkSize);
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

  @DisplayName("should return MARC_JSON as content type")
  @Test
  void shouldReturnMarcJsonContentType() {
    // arrange
    int chunkSize = 70;
    SourceReader reader = new MarcJsonReader(new File(SOURCE_PATH), chunkSize);

    // act
    String typeValue = reader.getContentType().toString();

    // assert
    assertThat(typeValue).isNotNull().isEqualTo(MARC_TYPE);
  }

  @DisplayName("should throw RecordsReaderException when source file does not exist")
  @Test
  void shouldThrowExceptionWhenSourceFileNotFound() {
    int chunkSize = 50;

    var file = new File(INCORRECT_SOURCE_PATH);
    assertThatThrownBy(() -> new MarcJsonReader(file, chunkSize))
      .isInstanceOf(RecordsReaderException.class);
  }

  @DisplayName("should throw RecordsReaderException when source file has wrong content type")
  @Test
  void shouldThrowExceptionOnHasNextWhenFileHasWrongContentType() {
    // arrange
    int chunkSize = 100;
    SourceReader reader = new MarcJsonReader(new File(INCORRECT_TYPE_SOURCE_PATH), chunkSize);

    // act & assert
    assertThatThrownBy(reader::hasNext)
      .isInstanceOf(RecordsReaderException.class);
  }

  @DisplayName("should return 3 chunks when chunk size is 21")
  @Test
  void shouldReturn3ChunksOfRecords() {
    // arrange
    int expectedChunksNumber = 3;
    int chunkSize = 21;
    SourceReader reader = new MarcJsonReader(new File(SOURCE_PATH), chunkSize);
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
}
