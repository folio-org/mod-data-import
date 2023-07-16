package org.folio.service.s3processing;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.service.s3storage.MinioStorageService;
import org.folio.service.s3storage.S3StorageWriter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;

@RunWith(VertxUnitRunner.class)
public class ChunkPlannerTest {
  private final Vertx vertx = Vertx.vertx();

  private MarcRawSplitterService marcRawSplitterService;

  private static final String VALID_MARC_SOURCE_PATH = "src/test/resources/100.mrc";
  private static final String INVALID_MARC_SOURCE_PATH = "src/test/resources/invalidMarcFile.mrc";

  private static final String VALID_MARC_SOURCE_PATH_10 = "src/test/resources/10.mrc";
  private static final String VALID_MARC_KEY = "100.mrc";

  private static final String VALID_MARC_KEY_10 = "10.mrc";
  private static final String INVALID_MARC_KEY = "invalidMarcFile.mrc";

  private static final int VALID_MARC_RECORD_COUNT = 100;
  private static final int VALID_MARC_RECORDS_PER_FILE = 25;

  @Mock
  private MinioStorageService minioStorageService;

  @Mock
  private S3StorageWriter partFileWriter;


  @Test
  public void shouldReturnReturnChunkPlanForValidMarcFile(TestContext context) throws IOException {


    BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(VALID_MARC_SOURCE_PATH_10));

    ChunkPlanner planner = new ChunkPlanner(VALID_MARC_KEY_10, inputStream,3);

    ChunkPlan plan = planner.planChunking();

    Map<Integer, ChunkPlanRecord> records = plan.getChunkPlanRecords();

    ChunkPlanRecord record = records.get(1);

    Map<Integer, ChunkPlanFile> files = plan.getChunkPlanFiles();

    ChunkPlanFile file = files.get(1);
  }

}
