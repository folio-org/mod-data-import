package org.folio.service.processing.reader;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.xlate.edi.internal.stream.tokenization.EDIException;
import io.xlate.edi.stream.EDIInputFactory;
import io.xlate.edi.stream.EDIStreamException;
import io.xlate.edi.stream.EDIStreamReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.folio.rest.jaxrs.model.InitialRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class EdifactReaderTest {

  private static final String PATH_TO_EDIFACT = "src/test/resources/edifact/";
  private static final String SOURCE_EMPTY = "empty.edi";

  static Stream<Arguments> edifactFiles() {
    return Stream.of(
      Arguments.of("CornAuxAm.1605541205.edi", 1),
      Arguments.of("CornCasalini.1606151339.edi", 1),
      Arguments.of("A-MGOBIe-orders565750us20200903.edi", 3),
      Arguments.of("CornHein1604419006.edi", 2),
      Arguments.of("AnneC-EBSCO-Access Only.INV", 1),
      Arguments.of("AnneC-EBSCO-Pkg & Item prices.INV", 1),
      Arguments.of("AnneC-EBSCO-Subns.INV", 1),
      Arguments.of("DukeEBSCOSubns.INV", 1),
      Arguments.of("TAMU-HRSW20200808072013.EDI", 7)
    );
  }

  @DisplayName("should return the expected number of records for each EDIFACT source file")
  @ParameterizedTest(name = "[{index}] {0} → {1} record(s)")
  @MethodSource("edifactFiles")
  void shouldReturnExpectedRecords_forEachEdifactFile(String fileName, int expectedRecordCount)
      throws EDIStreamException, FileNotFoundException {
    // arrange
    EDIInputFactory factory = EDIInputFactory.newFactory();
    List<String> expectedValidation = validateFile(factory, new FileInputStream(PATH_TO_EDIFACT + fileName));
    SourceReader reader = new EdifactReader(new File(PATH_TO_EDIFACT + fileName), 2);

    // act
    List<InitialRecord> actualRecords = readAllRecords(reader);

    // assert
    assertThat(actualRecords)
      .hasSize(expectedRecordCount)
      .allSatisfy(initialRecord -> assertThat(initialRecord.getOrder()).isNotNull());
    assertThat(validateFile(factory, new ByteArrayInputStream(
        actualRecords.getLast().getRecord().getBytes(StandardCharsets.UTF_8))))
      .isEqualTo(expectedValidation);
  }

  @DisplayName("should throw RecordsReaderException when reading an empty EDIFACT file")
  @Test
  void shouldThrowExceptionOnEmptyFile() {
    var file = new File(PATH_TO_EDIFACT + SOURCE_EMPTY);
    assertThatThrownBy(() -> new EdifactReader(file, 2)).isInstanceOf(RecordsReaderException.class);
  }

  private static List<InitialRecord> readAllRecords(SourceReader reader) {
    List<InitialRecord> records = new ArrayList<>();
    while (reader.hasNext()) {
      records.addAll(reader.next());
    }
    return records;
  }

  private List<String> validateFile(EDIInputFactory factory, InputStream fileContent) throws EDIStreamException {
    List<String> validationResults = new ArrayList<>();
    EDIStreamReader ediStreamReader = factory.createEDIStreamReader(fileContent, "ISO_8859_1");
    try {
      while (ediStreamReader.hasNext()) {
        switch (ediStreamReader.next()) {
          case ELEMENT_DATA_ERROR, SEGMENT_ERROR, ELEMENT_OCCURRENCE_ERROR:
            if (!ediStreamReader.getText().equals("ZZ")) {
              validationResults.add(ediStreamReader.getErrorType() + ":" + ediStreamReader.getText());
            }
            break;
          default:
            break;
        }
      }
    } catch (EDIException ex) {
      validationResults.add(ex.getMessage());
    }
    return validationResults;
  }
}
