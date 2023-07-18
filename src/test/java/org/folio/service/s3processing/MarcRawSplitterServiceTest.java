package org.folio.service.s3processing;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.folio.service.s3storage.MinioStorageService;
import org.folio.service.s3storage.S3StorageWriter;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;

import static org.mockito.ArgumentMatchers.anyString;

@RunWith(VertxUnitRunner.class)
public class MarcRawSplitterServiceTest {

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

  @Before
  public void setUp(TestContext context)  {
    MockitoAnnotations.openMocks(this);

    this.marcRawSplitterService = new MarcRawSplitterServiceImpl(vertx, minioStorageService);

    Mockito
      .doReturn(partFileWriter)
      .when(minioStorageService).writer(anyString());
  }


  @Test
  public void shouldReturnRecordCountForValidMarcFile(TestContext context) throws IOException {

    Async async = context.async();
    // given
    BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(VALID_MARC_SOURCE_PATH));

    // when
    Future<Integer> result = marcRawSplitterService.countRecordsInFile(inputStream);

    result.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      Integer count = ar.result();
      context.assertTrue(count == VALID_MARC_RECORD_COUNT);
      async.complete();
    });

    result.onFailure(_err ->
      context.fail("shouldReturnRecordCountForValidMarcFile should not fail")
    );
  }

  @Test
  public void shouldReturn0RecordsForInvalidMarcFile(TestContext context) throws IOException {
    Async async = context.async();
    // given
    BufferedInputStream inputStream = new BufferedInputStream(new FileInputStream(INVALID_MARC_SOURCE_PATH));

    // when
    Future<Integer> result = marcRawSplitterService.countRecordsInFile(inputStream);

    result.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      Integer count = ar.result();
      context.assertTrue(count == 0);
      async.complete();
    });

    result.onFailure(_err ->
      context.fail("shouldReturn0RecordsForInValidMarcFile should not fail")
    );
  }

}
