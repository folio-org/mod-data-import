package org.folio.service.processing.split;

import io.vertx.core.CompositeFuture;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import jakarta.validation.constraints.Min;
import javax.annotation.Nonnull;
import lombok.Builder;
import lombok.Data;
import org.folio.service.s3storage.MinioStorageService;

@Data
@Builder
public class FileSplitWriterOptions {

  /**
   * A promise that will resolve with a CompositeFuture containing either S3
   * keys or file paths to each chunk.
   */
  @Nonnull
  private final Promise<CompositeFuture> chunkUploadingCompositeFuturePromise;
  private MinioStorageService minioStorageService;
  @Builder.Default
  private Handler<Throwable> exceptionHandler = null;

  @Nonnull
  private String outputKey;

  /**
   * Where temporary files should be stored.
   */
  @Nonnull
  private String chunkFolder;

  @Min(1)
  private int maxRecordsPerChunk;

  @Builder.Default
  private boolean uploadFilesToS3 = true;

  @Builder.Default
  private boolean deleteLocalFiles = true;

  @Builder.Default
  private byte recordTerminator = FileSplitUtilities.MARC_RECORD_TERMINATOR;
}
