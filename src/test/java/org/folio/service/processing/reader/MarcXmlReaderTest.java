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
class MarcXmlReaderTest {
  private static final String SOURCE_PATH = "src/test/resources/UChicago_SampleBibs.xml";
  private static final String INCORRECT_SOURCE_PATH = "src/test/resources/wrong.json";
  private static final int EXPECTED_RECORDS_NUMBER = 62;
  private static final String MARC_TYPE = "MARC_XML";

  @DisplayName("should return all records from a MARC XML file")
  @Test
  void shouldReturnAllRecordsFromXmlFile() {
    // arrange
    int chunkSize = 100;
    SourceReader reader = new MarcXmlReader(new File(SOURCE_PATH), chunkSize);
    List<InitialRecord> actualRecords = new ArrayList<>();

    // act
    while (reader.hasNext()) {
      actualRecords.addAll(reader.next());
    }

    // assert
    assertThat(actualRecords).hasSize(EXPECTED_RECORDS_NUMBER);
  }

  @DisplayName("should return MARC_XML as content type")
  @Test
  void shouldReturnMarcXmlContentType() {
    // arrange
    int chunkSize = 50;
    SourceReader reader = new MarcXmlReader(new File(SOURCE_PATH), chunkSize);

    // act
    String typeValue = reader.getContentType().toString();

    // assert
    assertThat(typeValue).isNotNull().isEqualTo(MARC_TYPE);
  }

  @DisplayName("should throw RecordsReaderException when source file does not exist")
  @Test
  void shouldThrowExceptionWhenSourceFileNotFound() {
    int chunkSize = 40;

    var file = new File(INCORRECT_SOURCE_PATH);
    assertThatThrownBy(() -> new MarcXmlReader(file, chunkSize))
      .isInstanceOf(RecordsReaderException.class);
  }
}
