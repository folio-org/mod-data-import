package org.folio.service.processing.split;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;
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
   */
  public Future<List<String>> splitFileFromS3(Context context, String key) {
    return minioStorageService.readFile(key)
      // this stream will be closed as part of splitStream
      .compose((InputStream stream) -> splitStream(context, stream, key))
      .compose((List<String> result) -> {
        LOGGER.info("splitFileFromS3:: Split from S3 completed for key: {}. Deleting original file", key);
        return minioStorageService.remove(key).map(result);
      });
  }

  /**
   * Take a file, as an {@link InputStream}, split it into parts, and close it after.
   *
   * @return a {@link Future} which will resolve with a list of strings once every
   *         split chunk has been uploaded to MinIO/S3.
   */
  public Future<List<String>> splitStream(
    Context context,
    InputStream stream,
    String key
  ) {
    Promise<CompositeFuture> promise = Promise.promise();
    Future<Path> tempDirFeature = vertx.executeBlocking(() -> FileSplitUtilities.createTemporaryDir(key));

    return tempDirFeature.compose(tempDir -> {
      LOGGER.info("splitStream:: Streaming stream with key={} to writer, temporary folder={}...", key, tempDir);

      FileSplitWriter writer = new FileSplitWriter(
        FileSplitWriterOptions
          .builder()
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
      return asyncStream
        .pipeTo(writer)
        .andThen(ar -> asyncStream.close());
    })
      .onSuccess(v -> LOGGER.info("splitStream:: File split for key={} completed", key))
      // original future resolves once the chunks are split, but NOT uploaded
      .compose(v -> promise.future())
      // this composite future resolves once all are uploaded
      .compose(cf -> cf)
      // now let's turn this back into a List<String>
      .map(cf -> cf.list())
      .map(list -> list.stream().map(String.class::cast).toList())
      // and since we're all done, we can delete the temporary folder
      .compose((List<String> innerResult) -> {
        LOGGER.info("splitStream:: Deleting temporary folder={}", tempDirFeature.result());

        return vertx
          .fileSystem()
          .deleteRecursive(tempDirFeature.result().toString())
          .map(innerResult);
      })
      .onSuccess(result -> LOGGER.info("splitStream:: All done splitting! Got chunks {}", result))
      .onFailure(err -> LOGGER.error("splitStream:: Unable to split file: ", err));
  }
}
