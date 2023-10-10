package org.folio.service.processing.split;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class FileSplitUtilitiesCountTest {

  private static final JobProfileInfo MARC_PROFILE = new JobProfileInfo()
    .withDataType(JobProfileInfo.DataType.MARC);
  private static final JobProfileInfo EDIFACT_PROFILE = new JobProfileInfo()
    .withDataType(JobProfileInfo.DataType.EDIFACT);

  // tuples of [path, number of records, profile]
  @Parameters
  public static Collection<Object[]> getCases() {
    return Arrays.asList(
      new Object[] { "0.mrc", 0, MARC_PROFILE },
      // 1 buffer read
      new Object[] { "1.mrc", 1, MARC_PROFILE },
      // multiple buffer reads
      new Object[] { "100.mrc", 100, MARC_PROFILE },
      new Object[] { "2500.mrc", 2500, MARC_PROFILE },
      new Object[] { "5000.mrc", 5000, MARC_PROFILE },
      new Object[] { "10000.mrc", 10000, MARC_PROFILE },
      new Object[] { "22778.mrc", 22778, MARC_PROFILE },
      new Object[] { "50000.mrc", 50000, MARC_PROFILE },
      new Object[] { "invalidMarcFile.mrc", 0, MARC_PROFILE },
      // MARC XML
      new Object[] { "UChicago_SampleBibs.xml", 62, MARC_PROFILE },
      // MARC JSON
      new Object[] { "ChalmersFOLIOExamples.json", 62, MARC_PROFILE },
      // Edifact
      new Object[] { "edifact/TAMU-HRSW20200808072013.EDI", 7, EDIFACT_PROFILE }
    );
  }

  private String path;
  private int count;
  private JobProfileInfo profile;

  public FileSplitUtilitiesCountTest(
    String path,
    int count,
    JobProfileInfo profile
  ) {
    this.path = "src/test/resources/" + path;
    this.count = count;
    this.profile = profile;
  }

  @Test
  public void testCountAndStreamClose() throws IOException {
    BufferedInputStream inputStream = new BufferedInputStream(
      new FileInputStream(path)
    );

    assertThat(
      FileSplitUtilities.countRecordsInFile(path, inputStream, profile),
      is(count)
    );

    // countRecordsInMarcFile closes the stream, so it should be unavailable after
    // call, throwing an IOException
    assertThrows(IOException.class, () -> inputStream.available());
  }
}
