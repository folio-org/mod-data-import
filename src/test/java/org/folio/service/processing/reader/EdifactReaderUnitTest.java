package org.folio.service.processing.reader;

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
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RunWith(MockitoJUnitRunner.class)
public class EdifactReaderUnitTest {

  private static final String PATH_TO_EDIFACT = "src/test/resources/edifact/";
  private static final String SOURCE_PATH_FR = "CornAuxAm.1605541205.edi";
  private static final String SOURCE_PATH_IT = "CornCasalini.1606151339.edi";
  private static final String SOURCE_PATH_US_UK = "A-MGOBIe-orders565750us20200903.edi";
  private static final String SOURCE_PATH_CORN_HEIN = "CornHein1604419006.edi";
  private static final String SOURCE_PATH_EMPTY = "empty.edi";

  private Map<String, Integer> filesAndRecordsNumber = Map.of(SOURCE_PATH_FR, 1,
    SOURCE_PATH_IT, 1,
    SOURCE_PATH_US_UK, 3,
    SOURCE_PATH_CORN_HEIN, 2);

  @Test
  public void shouldReturnAllRecords() throws EDIStreamException, FileNotFoundException, UnsupportedEncodingException {
    // given
    boolean validationException = false;
    SourceReader reader;
    List<InitialRecord> actualRecords;
    EDIInputFactory factory = EDIInputFactory.newFactory();

    for (String fileName : filesAndRecordsNumber.keySet()) {
      // given
      System.out.printf("Handle: %s%n", fileName);
      reader = new EdifactReader(new File(PATH_TO_EDIFACT + fileName));

      //check files before tests
      System.out.println("\tValidating source file...");
      validateFile(factory, new FileInputStream(PATH_TO_EDIFACT + fileName));

      // when
      actualRecords = new ArrayList<>(reader.next());

      // then
      Assert.assertEquals("File: " + fileName, filesAndRecordsNumber.get(fileName).intValue(), actualRecords.size());

      System.out.println("\tValidating parsing result...");
      for (InitialRecord initialRecord : actualRecords) {
        validationException = validateFile(factory,
          new ByteArrayInputStream(initialRecord.getRecord().getBytes(StandardCharsets.UTF_8)));
      }
    }
    //Assert.assertEquals(false, validationException);
  }

  @Test(expected = IllegalArgumentException.class)
  public void shouldThrowExceptionOnEmptyFile() throws EDIStreamException, FileNotFoundException, UnsupportedEncodingException {

    // given
    SourceReader reader = new EdifactReader(new File(PATH_TO_EDIFACT + SOURCE_PATH_EMPTY));

    // then
    reader.next();
  }

  private boolean validateFile(EDIInputFactory factory, InputStream fileContent) throws EDIStreamException {
    boolean validationException = true;

    EDIStreamReader ediStreamReader = factory.createEDIStreamReader(fileContent, "ISO_8859_1");
    while (ediStreamReader.hasNext()) {
      switch (ediStreamReader.next()) {
        case ELEMENT_DATA_ERROR:
        case SEGMENT_ERROR:
        case ELEMENT_OCCURRENCE_ERROR:
          if (!ediStreamReader.getText().equals("ZZ")) {
            System.out.println(String.format("\t\t %s -> %s", ediStreamReader.getErrorType(), ediStreamReader.getText()));
            validationException = true;
          }
        default:
          break;
      }
    }
    return validationException;
  }

}
