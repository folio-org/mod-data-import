package org.folio.service.processing.split;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class FileSplitUtilitiesIsBinaryMarcTest {

  private static final JobProfileInfo MARC_PROFILE = new JobProfileInfo()
    .withDataType(JobProfileInfo.DataType.MARC);
  private static final JobProfileInfo EDIFACT_PROFILE = new JobProfileInfo()
    .withDataType(JobProfileInfo.DataType.EDIFACT);

  // tuples of [path, profile, expected]
  @Parameters
  public static Collection<Object[]> getCases() {
    return Arrays.asList(
      new Object[] { "test.mrc", MARC_PROFILE, true },
      new Object[] { "test.mrc21", MARC_PROFILE, true },
      // specifically excludes MARC JSON/XML
      new Object[] { "test.json", MARC_PROFILE, false },
      new Object[] { "test.xml", MARC_PROFILE, false },
      // non-marc profile takes precedence
      new Object[] { "test.mrc", EDIFACT_PROFILE, false }
    );
  }

  private String path;
  private JobProfileInfo profile;
  private boolean expected;

  public FileSplitUtilitiesIsBinaryMarcTest(
    String path,
    JobProfileInfo profile,
    boolean expected
  ) {
    this.path = "src/test/resources/" + path;
    this.profile = profile;
    this.expected = expected;
  }

  @Test
  public void testIsBinaryMarcFile() throws IOException {
    assertThat(FileSplitUtilities.isMarcBinary(path, profile), is(expected));
  }
}
