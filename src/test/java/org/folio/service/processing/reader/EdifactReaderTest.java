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
import java.util.Map;
import org.folio.rest.jaxrs.model.InitialRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class EdifactReaderTest {

  private static final String PATH_TO_EDIFACT = "src/test/resources/edifact/";
  private static final String SOURCE_FR = "CornAuxAm.1605541205.edi";
  private static final String SOURCE_IT = "CornCasalini.1606151339.edi";
  private static final String SOURCE_US_UK = "A-MGOBIe-orders565750us20200903.edi";
  private static final String SOURCE_CORN_HEIN = "CornHein1604419006.edi";
  private static final String SOURCE_EBSCO_1 = "AnneC-EBSCO-Access Only.INV";
  private static final String SOURCE_EBSCO_2 = "AnneC-EBSCO-Pkg & Item prices.INV";
  private static final String SOURCE_EBSCO_3 = "AnneC-EBSCO-Subns.INV";
  private static final String SOURCE_EBSCO_4 = "DukeEBSCOSubns.INV";
  private static final String SOURCE_TAMU = "TAMU-HRSW20200808072013.EDI";

  private static final String SOURCE_EMPTY = "empty.edi";

  private final Map<String, Integer> filesAndRecordsNumber = Map.of(
    SOURCE_FR, 1, SOURCE_IT, 1,
    SOURCE_US_UK, 3, SOURCE_CORN_HEIN, 2,
    SOURCE_EBSCO_1, 1, SOURCE_EBSCO_2, 1,
    SOURCE_EBSCO_3, 1, SOURCE_EBSCO_4, 1,
    SOURCE_TAMU, 7);

  @DisplayName("should return the expected number of records for each EDIFACT source file")
  @Test
  void shouldReturnAllRecords() throws EDIStreamException, FileNotFoundException {
    EDIInputFactory factory = EDIInputFactory.newFactory();

    for (String fileName : filesAndRecordsNumber.keySet()) {
      SourceReader reader = new EdifactReader(new File(PATH_TO_EDIFACT + fileName), 2);

      final var expValidation = validateFile(factory, new FileInputStream(PATH_TO_EDIFACT + fileName));

      List<InitialRecord> actualRecords = new ArrayList<>();
      while (reader.hasNext()) {
        actualRecords.addAll(reader.next());
      }

      assertThat(actualRecords).as("File: " + fileName)
        .hasSize(filesAndRecordsNumber.get(fileName));

      List<String> actValidation = new ArrayList<>();
      for (InitialRecord initialRecord : actualRecords) {
        actValidation = validateFile(factory,
          new ByteArrayInputStream(initialRecord.getRecord().getBytes(StandardCharsets.UTF_8)));
        assertThat(initialRecord.getOrder()).as("Order is null").isNotNull();
      }
      assertThat(actValidation).as("File: " + fileName).isEqualTo(expValidation);
    }
  }

  @DisplayName("should throw RecordsReaderException when reading an empty EDIFACT file")
  @Test
  void shouldThrowExceptionOnEmptyFile() {
    var file = new File(PATH_TO_EDIFACT + SOURCE_EMPTY);
    assertThatThrownBy(() -> new EdifactReader(file, 2)).isInstanceOf(RecordsReaderException.class);
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
