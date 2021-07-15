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
  private static final String SOURCE_EDIFACT_PATH = "src/test/resources/edifact/CornHein1604419006.edi";
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
    Assert.assertEquals(expectedMarcType, reader.getContentType().toString());
  }

  @Test
  public void buildShouldReturnMarcXmlReader() {
    //given
    String expectedMarcType = "MARC_XML";
    //when
    SourceReader reader = SourceReaderBuilder.build(new File(SOURCE_XML_PATH), marcJobProfile);
    //then
    Assert.assertEquals(expectedMarcType, reader.getContentType().toString());
  }

  @Test
  public void buildShouldReturnMarcRawReader() {
    //given
    String expectedMarcType = "MARC_RAW";
    //when
    SourceReader reader = SourceReaderBuilder.build(new File(SOURCE_MARC_PATH), marcJobProfile);
    //then
    Assert.assertEquals(expectedMarcType, reader.getContentType().toString());
  }

  @Test
  public void buildShouldReturnEdifactReader() {
    //given
    String expectedEdifactType = "EDIFACT_RAW";
    //when
    SourceReader reader = SourceReaderBuilder.build(new File(SOURCE_EDIFACT_PATH), edifactJobProfile);
    //then
    Assert.assertEquals(expectedEdifactType, reader.getContentType().toString());
  }
}
