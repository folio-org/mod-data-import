package org.folio.service.processing.split;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.Arrays;
import java.util.Collection;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Parameterized.class)
public class FileSplitUtilitiesChunkKeyTest {

  // tuples of [key, part number, expected]
  @Parameters
  public static Collection<Object[]> getCases() {
    return Arrays.asList(
      new Object[] { "test", 1, "test_1" },
      new Object[] { "test.mrc", 1, "test_1.mrc" },
      new Object[] { "test.mrc", 234, "test_234.mrc" },
      new Object[] { "test.foo.mrc", 1, "test.foo_1.mrc" },
      new Object[] { "test.foo_12.mrc", 2, "test.foo_12_2.mrc" },
      new Object[] {
        "a/really.long/and_..complex.path",
        15,
        "a/really.long/and_..complex_15.path",
      },
      new Object[] { "windows\\style.path", 128, "windows\\style_128.path" }
    );
  }

  private String key;
  private int partNumber;
  private String expected;

  public FileSplitUtilitiesChunkKeyTest(
    String key,
    int partNumber,
    String expected
  ) {
    this.key = key;
    this.partNumber = partNumber;
    this.expected = expected;
  }

  @Test
  public void testChunkKeyGeneration() {
    assertThat(FileSplitUtilities.buildChunkKey(key, partNumber), is(expected));
  }
}
