package org.folio.service.processing.reader;

import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(MockitoExtension.class)
class SourceReaderBuilderTest {
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

  @BeforeEach
  void setUp() {
    marcJobProfile = new JobProfileInfo();
    marcJobProfile.setDataType(JobProfileInfo.DataType.MARC);
    edifactJobProfile = new JobProfileInfo();
    edifactJobProfile.setDataType(JobProfileInfo.DataType.EDIFACT);
  }

  @DisplayName("should return MarcJsonReader when source file is JSON")
  @Test
  void shouldBuildMarcJsonReader() {
    // arrange
    String expectedMarcType = "MARC_JSON";

    // act
    SourceReader reader = SourceReaderBuilder.build(new File(SOURCE_JSON_PATH), marcJobProfile);

    // assert
    assertThat(reader.getContentType()).hasToString(expectedMarcType);
  }

  @DisplayName("should return MarcXmlReader when source file is XML")
  @Test
  void shouldBuildMarcXmlReader() {
    // arrange
    String expectedMarcType = "MARC_XML";

    // act
    SourceReader reader = SourceReaderBuilder.build(new File(SOURCE_XML_PATH), marcJobProfile);

    // assert
    assertThat(reader.getContentType()).hasToString(expectedMarcType);
  }

  @DisplayName("should return MarcRawReader when source file is MRC")
  @Test
  void shouldBuildMarcRawReader() {
    // arrange
    String expectedMarcType = "MARC_RAW";

    // act
    SourceReader reader = SourceReaderBuilder.build(new File(SOURCE_MARC_PATH), marcJobProfile);

    // assert
    assertThat(reader.getContentType()).hasToString(expectedMarcType);
  }

  @DisplayName("should return EdifactReader for file with uppercase .EDI extension")
  @Test
  void shouldBuildEdifactReaderForFileWithEdiUpperCaseExtension() {
    // act
    SourceReader reader = SourceReaderBuilder.build(new File(SOURCE_EDIFACT_PATH_UPPER_CASE_EDI_EXTENSION), edifactJobProfile);

    // assert
    assertThat(reader.getContentType()).hasToString(EXPECTED_EDIFACT_TYPE);
  }

  @DisplayName("should return EdifactReader for file with lowercase .edi extension")
  @Test
  void shouldBuildEdifactReaderForFileWithEdiLowerCaseExtension() {
    // act
    SourceReader reader = SourceReaderBuilder.build(new File(SOURCE_EDIFACT_PATH_LOW_CASE_EDI_EXTENSION), edifactJobProfile);

    // assert
    assertThat(reader.getContentType()).hasToString(EXPECTED_EDIFACT_TYPE);
  }

  @DisplayName("should return EdifactReader for file with mixed-case .eDI extension")
  @Test
  void shouldBuildEdifactReaderForFileWithEdiMixedCaseExtension() {
    // act
    SourceReader reader = SourceReaderBuilder.build(new File(SOURCE_EDIFACT_PATH_MIXED_CASE_EDI_EXTENSION), edifactJobProfile);

    // assert
    assertThat(reader.getContentType()).hasToString(EXPECTED_EDIFACT_TYPE);
  }

  @DisplayName("should return EdifactReader for file with lowercase .inv extension")
  @Test
  void shouldBuildEdifactReaderForFileWithInvLowerCaseExtension() {
    // act
    SourceReader reader = SourceReaderBuilder.build(new File(SOURCE_EDIFACT_PATH_LOW_CASE_INV_EXTENSION), edifactJobProfile);

    // assert
    assertThat(reader.getContentType()).hasToString(EXPECTED_EDIFACT_TYPE);
  }

  @DisplayName("should return EdifactReader for file with uppercase .INV extension")
  @Test
  void shouldBuildEdifactReaderForFileWithInvUpperCaseExtension() {
    // act
    SourceReader reader = SourceReaderBuilder.build(new File(SOURCE_EDIFACT_PATH_UPPER_CASE_INV_EXTENSION), edifactJobProfile);

    // assert
    assertThat(reader.getContentType()).hasToString(EXPECTED_EDIFACT_TYPE);
  }

  @DisplayName("should return EdifactReader for file with mixed-case .InV extension")
  @Test
  void shouldBuildEdifactReaderForFileWithInvMixedCaseExtension() {
    // act
    SourceReader reader = SourceReaderBuilder.build(new File(SOURCE_EDIFACT_PATH_MIXED_CASE_INV_EXTENSION), edifactJobProfile);

    // assert
    assertThat(reader.getContentType()).hasToString(EXPECTED_EDIFACT_TYPE);
  }
}
