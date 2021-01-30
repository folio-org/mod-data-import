package org.folio.service.processing.reader;

import io.xlate.edi.internal.stream.tokenization.EDIException;
import io.xlate.edi.stream.EDIInputFactory;
import io.xlate.edi.stream.EDIStreamException;
import io.xlate.edi.stream.EDIStreamReader;
import org.folio.rest.jaxrs.model.InitialRecord;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class EdifactReaderUnitTest {

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

  private Map<String, Integer> filesAndRecordsNumber = Map.of(
    SOURCE_FR, 1, SOURCE_IT, 1,
    SOURCE_US_UK, 3, SOURCE_CORN_HEIN, 2,
    SOURCE_EBSCO_1, 1, SOURCE_EBSCO_2, 1,
    SOURCE_EBSCO_3, 1, SOURCE_EBSCO_4, 1,
    SOURCE_TAMU, 7);

  @Test
  public void shouldReturnAllRecords() throws EDIStreamException, FileNotFoundException {

    // given
    SourceReader reader;
    List<InitialRecord> actualRecords;
    EDIInputFactory factory = EDIInputFactory.newFactory();

    for (String fileName : filesAndRecordsNumber.keySet()) {
      // given
      System.out.printf("Handle: %s%n", fileName);
      reader = new EdifactReader(new File(PATH_TO_EDIFACT + fileName), 2);

      //check files before tests
      System.out.println("\tValidating source file...");
      List<String> expValidation = validateFile(factory, new FileInputStream(PATH_TO_EDIFACT + fileName));

      // when
      actualRecords = new ArrayList<>();
      while (reader.hasNext()) {
        actualRecords.addAll(reader.next());
      }

      // then
      Assert.assertEquals("File: " + fileName, filesAndRecordsNumber.get(fileName).intValue(), actualRecords.size());

      List<String> actValidation = new ArrayList<>();
      System.out.println("\tValidating parsing result...");
      for (InitialRecord initialRecord : actualRecords) {
        actValidation = validateFile(factory,
          new ByteArrayInputStream(initialRecord.getRecord().getBytes(StandardCharsets.UTF_8)));
        Assert.assertNotNull("Order is null", initialRecord.getOrder());
        Assert.assertTrue("Order is less than 0",initialRecord.getOrder() > 0);
      }
      Assert.assertEquals("File: " + fileName, expValidation, actValidation);
    }
  }

  @Test(expected = RecordsReaderException.class)
  public void shouldThrowExceptionOnEmptyFile() {

    // given
    SourceReader reader = new EdifactReader(new File(PATH_TO_EDIFACT + SOURCE_EMPTY), 2);

    // then
    reader.next();
  }

  private List<String> validateFile(EDIInputFactory factory, InputStream fileContent) throws EDIStreamException {
    List<String> validationResults = new ArrayList<>();
    EDIStreamReader ediStreamReader = factory.createEDIStreamReader(fileContent, "ISO_8859_1");
    try {
      while (ediStreamReader.hasNext()) {
        switch (ediStreamReader.next()) {
          case ELEMENT_DATA_ERROR:
          case SEGMENT_ERROR:
          case ELEMENT_OCCURRENCE_ERROR:
            if (!ediStreamReader.getText().equals("ZZ")) {
              System.out.printf("\t\t %s -> %s%n", ediStreamReader.getErrorType(), ediStreamReader.getText());
              validationResults.add(ediStreamReader.getErrorType() + ":" + ediStreamReader.getText());
            }
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
