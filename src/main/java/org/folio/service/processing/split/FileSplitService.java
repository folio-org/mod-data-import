package org.folio.service.processing.split;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.service.s3storage.MinioStorageService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class FileSplitService {

  private static final Logger LOGGER = LogManager.getLogger();

  private MinioStorageService minioStorageService;

  private int maxRecordsPerChunk;

  @Autowired
  public FileSplitService(MinioStorageService minioStorageService,
      @Value("${RECORDS_PER_SPLIT_FILE:1000}") int maxRecordsPerChunk) {
    this.minioStorageService = minioStorageService;
    this.maxRecordsPerChunk = maxRecordsPerChunk;
  }

  /**
   * Read a file from S3 and split it into parts.
   *
   * @return a {@link Promise} that will be completed when the file has been
   *         split. This
   *         promise wraps a {@link CompositeFuture} which will resolve once every
   *         split chunk
   *         has been uploaded to MinIO/S3.
   * @throws IOException if the file cannot be read or if temporary files cannot
   *                     be created
   */
  public Future<CompositeFuture> splitFileFromS3(Context context, String key) {
    return minioStorageService
        .readFile(key)
        .compose(stream -> {
          try {
            return splitStream(context, stream, key);
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        });
  }

  /**
   * Take a file, as an {@link InputStream}, and split it into parts.
   *
   * @return a {@link Future} that will be completed when the file has been
   *         split. This
   *         promise wraps a {@link CompositeFuture} which will resolve once every
   *         split chunk
   *         has been uploaded to MinIO/S3.
   * @throws IOException if the stream cannot be read or if temporary files cannot
   *                     be created
   */
  public Future<CompositeFuture> splitStream(
      Context context,
      InputStream stream,
      String key) throws IOException {
    Promise<CompositeFuture> promise = Promise.promise();

    LOGGER.info("Streaming stream with key={} to writer...", key);

    FileSplitWriter writer = new FileSplitWriter(
        context,
        promise,
        key,
        "",
        maxRecordsPerChunk,
        FileSplitUtilities.MARC_RECORD_TERMINATOR,
        true,
        true);

    new AsyncInputStream(context.owner(), context, stream)
        .pipeTo(writer)
        .onComplete(ar1 -> LOGGER.info("File split for key={} completed", key));

    return promise.future();
  }
}
