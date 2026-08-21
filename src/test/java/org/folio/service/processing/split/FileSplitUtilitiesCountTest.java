package org.folio.service.processing.split;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.stream.Stream;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class FileSplitUtilitiesCountTest {

  private static final JobProfileInfo MARC_PROFILE = new JobProfileInfo()
    .withDataType(JobProfileInfo.DataType.MARC);
  private static final JobProfileInfo EDIFACT_PROFILE = new JobProfileInfo()
    .withDataType(JobProfileInfo.DataType.EDIFACT);

  static Stream<Arguments> getCases() {
    return Stream.of(
      Arguments.of("0.mrc", 0, MARC_PROFILE),
      Arguments.of("1.mrc", 1, MARC_PROFILE),
      Arguments.of("100.mrc", 100, MARC_PROFILE),
      Arguments.of("2500.mrc", 2500, MARC_PROFILE),
      Arguments.of("5000.mrc", 5000, MARC_PROFILE),
      Arguments.of("10000.mrc", 10000, MARC_PROFILE),
      Arguments.of("22778.mrc", 22778, MARC_PROFILE),
      Arguments.of("50000.mrc", 50000, MARC_PROFILE),
      Arguments.of("invalidMarcFile.mrc", 0, MARC_PROFILE),
      Arguments.of("UChicago_SampleBibs.xml", 62, MARC_PROFILE),
      Arguments.of("ChalmersFOLIOExamples.json", 62, MARC_PROFILE),
      Arguments.of("edifact/TAMU-HRSW20200808072013.EDI", 7, EDIFACT_PROFILE)
    );
  }

  @ParameterizedTest
  @MethodSource("getCases")
  @DisplayName("should count records in file and close the stream")
  void shouldCountRecords_andCloseStream(
    String path,
    int count,
    JobProfileInfo profile
  ) throws IOException {
    String fullPath = "src/test/resources/" + path;
    BufferedInputStream inputStream = new BufferedInputStream(
      new FileInputStream(fullPath)
    );

    assertThat(FileSplitUtilities.countRecordsInFile(fullPath, inputStream, profile))
      .isEqualTo(count);

    assertThatThrownBy(inputStream::available)
      .isInstanceOf(IOException.class);
  }
}
