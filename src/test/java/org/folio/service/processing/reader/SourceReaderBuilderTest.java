package org.folio.service.processing.reader;

import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.io.File;

/**
 * Testing SourceReaderBuilder
 */
@RunWith(MockitoJUnitRunner.class)
public class SourceReaderBuilderTest {
  private static final String SOURCE_XML_PATH = "src/test/resources/UChicago_SampleBibs.xml";
  private static final String SOURCE_JSON_PATH = "src/test/resources/ChalmersFOLIOExamples.json";
  private static final String SOURCE_MARC_PATH = "src/test/resources/CornellFOLIOExemplars.mrc";
  private static final String SOURCE_EDIFACT_PATH_LOW_CASE_EDI_EXTENSION = "src/test/resources/edifact/CornHein1604419006.edi";
  private static final String SOURCE_EDIFACT_PATH_UPPER_CASE_EDI_EXTENSION = "src/test/resources/edifact/CornHein1604419007.EDI";
  private static final String SOURCE_EDIFACT_PATH_MIXED_CASE_EDI_EXTENSION = "src/test/resources/edifact/CornHein1604419008.eDI";
  private static final String SOURCE_EDIFACT_PATH_LOW_CASE_INV_EXTENSION = "src/test/resources/edifact/AnneC-EBSCO-Subns1.inv";
  private static final String SOURCE_EDIFACT_PATH_UPPER_CASE_INV_EXTENSION = "src/test/resources/edifact/AnneC-EBSCO-Subns.INV";
  private static final String SOURCE_EDIFACT_PATH_MIXED_CASE_INV_EXTENSION = "src/test/resources/edifact/AnneC-EBSCO-Subns2.InV";
  private static final String EXPECTED_EDIFACT_TYPE = "EDIFACT_RAW";
  private JobProfileInfo marcJobProfile;
  private JobProfileInfo edifactJobProfile;

  @Before
  public void setUp() {
    marcJobProfile = new JobProfileInfo();
    marcJobProfile.setDataType(JobProfileInfo.DataType.MARC);
    edifactJobProfile = new JobProfileInfo();
    edifactJobProfile.setDataType(JobProfileInfo.DataType.EDIFACT);
  }

  @Test
  public void buildShouldReturnMarcJsonReader() {
    //given
    String expectedMarcType = "MARC_JSON";
    //when
    SourceReader reader = SourceReaderBuilder.build(new File(SOURCE_JSON_PATH), marcJobProfile);
    //then
    Assert.assertNotNull(reader);
    Assert.assertEquals(expectedMarcType, reader.getContentType().toString());
  }

  @Test
  public void buildShouldReturnMarcXmlReader() {
    //given
    String expectedMarcType = "MARC_XML";
    //when
    SourceReader reader = SourceReaderBuilder.build(new File(SOURCE_XML_PATH), marcJobProfile);
    //then
    Assert.assertNotNull(reader);
    Assert.assertEquals(expectedMarcType, reader.getContentType().toString());
  }

  @Test
  public void buildShouldReturnMarcRawReader() {
    //given
    String expectedMarcType = "MARC_RAW";
    //when
    SourceReader reader = SourceReaderBuilder.build(new File(SOURCE_MARC_PATH), marcJobProfile);
    //then
    Assert.assertNotNull(reader);
    Assert.assertEquals(expectedMarcType, reader.getContentType().toString());
  }

  @Test
  public void buildShouldReturnEdifactReaderForFileWithEdiLowerCaseExtension() {
    //given
    SourceReader reader;
    //when
    reader = SourceReaderBuilder.build(new File(SOURCE_EDIFACT_PATH_UPPER_CASE_EDI_EXTENSION), edifactJobProfile);
    //then
    Assert.assertNotNull(reader);
    Assert.assertEquals(EXPECTED_EDIFACT_TYPE, reader.getContentType().toString());
  }

  @Test
  public void buildShouldReturnEdifactReaderForFileWithEdiUpperCaseExtension() {
    //given
    SourceReader reader;
    //when
    reader = SourceReaderBuilder.build(new File(SOURCE_EDIFACT_PATH_LOW_CASE_EDI_EXTENSION), edifactJobProfile);
    //then
    Assert.assertNotNull(reader);
    Assert.assertEquals(EXPECTED_EDIFACT_TYPE, reader.getContentType().toString());
  }

  @Test
  public void buildShouldReturnEdifactReaderForFileWithEdiMixedCaseExtension() {
    //given
    SourceReader reader;
    //when
    reader = SourceReaderBuilder.build(new File(SOURCE_EDIFACT_PATH_MIXED_CASE_EDI_EXTENSION), edifactJobProfile);
    //then
    Assert.assertNotNull(reader);
    Assert.assertEquals(EXPECTED_EDIFACT_TYPE, reader.getContentType().toString());
  }

  @Test
  public void buildShouldReturnEdifactReaderForFileWithInvLowerCaseExtension() {
    //given
    SourceReader reader;
    //when
    reader = SourceReaderBuilder.build(new File(SOURCE_EDIFACT_PATH_LOW_CASE_INV_EXTENSION), edifactJobProfile);
    //then
    Assert.assertNotNull(reader);
    Assert.assertEquals(EXPECTED_EDIFACT_TYPE, reader.getContentType().toString());
  }

  @Test
  public void buildShouldReturnEdifactReaderForFileWithInvUpperCaseExtension() {
    //given
    SourceReader reader;
    //when
    reader = SourceReaderBuilder.build(new File(SOURCE_EDIFACT_PATH_UPPER_CASE_INV_EXTENSION), edifactJobProfile);
    //then
    Assert.assertNotNull(reader);
    Assert.assertEquals(EXPECTED_EDIFACT_TYPE, reader.getContentType().toString());
  }

  @Test
  public void buildShouldReturnEdifactReaderForFileWithInvMixedCaseExtension() {
    //given
    SourceReader reader;
    //when
    reader = SourceReaderBuilder.build(new File(SOURCE_EDIFACT_PATH_MIXED_CASE_INV_EXTENSION), edifactJobProfile);
    //then
    Assert.assertNotNull(reader);
    Assert.assertEquals(EXPECTED_EDIFACT_TYPE, reader.getContentType().toString());
  }
}
