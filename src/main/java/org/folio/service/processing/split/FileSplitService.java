package org.folio.service.processing.split;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.folio.service.s3storage.MinioStorageService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Service
public class FileSplitService {

  private static final Logger LOGGER = LogManager.getLogger();

  private Vertx vertx;

  private MinioStorageService minioStorageService;

  private int maxRecordsPerChunk;

  @Autowired
  public FileSplitService(
    Vertx vertx,
    MinioStorageService minioStorageService,
    @Value("${RECORDS_PER_SPLIT_FILE:1000}") int maxRecordsPerChunk
  ) {
    this.vertx = vertx;
    this.minioStorageService = minioStorageService;
    this.maxRecordsPerChunk = maxRecordsPerChunk;
  }

  /**
   * Read a file from S3 and split it into parts.
   *
   * @return a {@link Promise} that wraps a list of string keys and will resolve
   *         once every split chunk has been uploaded to MinIO/S3.
   * @throws IOException if the file cannot be read or if temporary files cannot
   *                     be created
   */
  public Future<List<String>> splitFileFromS3(Context context, String key) {
    return minioStorageService
      .readFile(key)
      .compose((InputStream stream) -> {
        // this stream will be closed as part of splitStream
        try {
          return splitStream(context, stream, key)
            .compose((List<String> result) -> {
              LOGGER.info("Split from S3 completed...deleting original file");
              return minioStorageService.remove(key).map(v -> result);
            });
        } catch (IOException e) {
          LOGGER.error("Unable to split file", e);
          throw new UncheckedIOException(e);
        }
      });
  }

  /**
   * Take a file, as an {@link InputStream}, split it into parts, and close it after.
   *
   * @return a {@link Future} which will resolve with a list of strings once every
   *         split chunk has been uploaded to MinIO/S3.
   * @throws IOException if the stream cannot be read or if temporary files cannot
   *                     be created
   */
  public Future<List<String>> splitStream(
    Context context,
    InputStream stream,
    String key
  ) throws IOException {
    Promise<CompositeFuture> promise = Promise.promise();

    Path tempDir = FileSplitUtilities.createTemporaryDir(key);

    LOGGER.info(
      "Streaming stream with key={} to writer, temporary folder={}...",
      key,
      tempDir
    );

    FileSplitWriter writer = new FileSplitWriter(
      FileSplitWriterOptions
        .builder()
        .vertxContext(context)
        .minioStorageService(minioStorageService)
        .chunkUploadingCompositeFuturePromise(promise)
        .outputKey(key)
        .chunkFolder(tempDir.toString())
        .maxRecordsPerChunk(maxRecordsPerChunk)
        .uploadFilesToS3(true)
        .deleteLocalFiles(true)
        .build()
    );

    AsyncInputStream asyncStream = new AsyncInputStream(context, stream);
    asyncStream
      .pipeTo(writer)
      .onComplete(ar1 -> LOGGER.info("File split for key={} completed", key));

    return promise
      // original future resolves once the chunks are split, but NOT uploaded
      .future()
      // this composite future resolves once all are uploaded
      .compose(cf -> cf)
      // now let's turn this back into a List<String>
      .map(cf -> cf.list())
      .map(list ->
        list.stream().map(String.class::cast).collect(Collectors.toList())
      )
      // and since we're all done, we can delete the temporary folder
      .compose((List<String> innerResult) -> {
        LOGGER.info("Deleting temporary folder={}", tempDir);

        return vertx
          .fileSystem()
          .deleteRecursive(tempDir.toString(), true)
          .map(v -> innerResult);
      })
      .onSuccess(result ->
        LOGGER.info("All done splitting! Got chunks {}", result)
      )
      .onFailure(err -> LOGGER.error("Unable to split file: ", err))
      .onComplete(v -> asyncStream.close());
  }
}
