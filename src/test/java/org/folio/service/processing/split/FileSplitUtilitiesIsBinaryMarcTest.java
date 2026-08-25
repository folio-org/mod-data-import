package org.folio.service.processing.split;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.stream.Stream;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class FileSplitUtilitiesIsBinaryMarcTest {

  private static final JobProfileInfo MARC_PROFILE = new JobProfileInfo()
    .withDataType(JobProfileInfo.DataType.MARC);
  private static final JobProfileInfo EDIFACT_PROFILE = new JobProfileInfo()
    .withDataType(JobProfileInfo.DataType.EDIFACT);

  static Stream<Arguments> getCases() {
    return Stream.of(
      Arguments.of("test.mrc", MARC_PROFILE, true),
      Arguments.of("test.mrc21", MARC_PROFILE, true),
      Arguments.of("test.json", MARC_PROFILE, false),
      Arguments.of("test.xml", MARC_PROFILE, false),
      Arguments.of("test.mrc", EDIFACT_PROFILE, false)
    );
  }

  @ParameterizedTest
  @MethodSource("getCases")
  @DisplayName("should determine if file is binary MARC based on extension and profile")
  void shouldDetermineIsMarcBinary(String path, JobProfileInfo profile, boolean expected) {
    assertThat(FileSplitUtilities.isMarcBinary("src/test/resources/" + path, profile))
      .isEqualTo(expected);
  }
}
