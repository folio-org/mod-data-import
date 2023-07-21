package org.folio.service.processing.split;

import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

@RunWith(VertxUnitRunner.class)
public class FileSplitUtilitiesTest {


  private static final String VALID_MARC_SOURCE_PATH = "src/test/resources/100.mrc";
  private static final String INVALID_MARC_SOURCE_PATH = "src/test/resources/invalidMarcFile.mrc";

  private static final int VALID_MARC_RECORD_COUNT = 100;

  @Test
  public void shouldReturnRecordCountForValidMarcFile(TestContext context) throws IOException {
    BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(VALID_MARC_SOURCE_PATH));

    Integer count = FileSplitUtilities.countRecordsInMarcFile(inputStream);

    assertEquals(VALID_MARC_RECORD_COUNT, count.intValue());

  }

  @Test
  public void shouldReturn0RecordsForInvalidMarcFile(TestContext context) throws IOException {

    BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(INVALID_MARC_SOURCE_PATH));

    Integer count = FileSplitUtilities.countRecordsInMarcFile(inputStream);

    assertEquals(0, count.intValue());
  }

}
