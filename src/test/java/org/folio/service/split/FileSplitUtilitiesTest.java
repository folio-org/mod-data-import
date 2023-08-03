package org.folio.service.split;

import static org.junit.Assert.assertEquals;

import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import org.folio.service.processing.split.FileSplitUtilities;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class FileSplitUtilitiesTest {

  private static final String VALID_MARC_SOURCE_PATH = "src/test/resources/100.mrc";
  private static final String INVALID_MARC_SOURCE_PATH = "src/test/resources/invalidMarcFile.mrc";

  private static final int VALID_MARC_RECORD_COUNT = 100;

  @Test
  public void shouldReturnRecordCountForValidMarcFile(TestContext context)
      throws IOException {
    BufferedInputStream inputStream = new BufferedInputStream(
        new FileInputStream(VALID_MARC_SOURCE_PATH));

    Integer count = FileSplitUtilities.countRecordsInMarcFile(inputStream);

    assertEquals(VALID_MARC_RECORD_COUNT, count.intValue());
  }

  @Test
  public void shouldReturn0RecordsForInvalidMarcFile(TestContext context)
      throws IOException {
    BufferedInputStream inputStream = new BufferedInputStream(
        new FileInputStream(INVALID_MARC_SOURCE_PATH));

    Integer count = FileSplitUtilities.countRecordsInMarcFile(inputStream);

    assertEquals(0, count.intValue());
  }
}
