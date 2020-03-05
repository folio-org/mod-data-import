package org.folio.service.processing;

import io.vertx.codegen.annotations.ProxyGen;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

/**
 * Processing files associated with given request.
 */
@ProxyGen
public interface FileProcessor { //NOSONAR

  String FILE_PROCESSOR_ADDRESS = "file-processor.queue"; //NOSONAR

  static FileProcessor create(Vertx vertx) {
    return new ParallelFileChunkingProcessor(vertx);
  }

  static FileProcessor createProxy(Vertx vertx) {
    return new FileProcessorVertxEBProxy(vertx, FILE_PROCESSOR_ADDRESS);
  }

  /**
   * Performs processing files related to given request,
   * sets JobExecution status to ERROR in case file processing failed
   *
   * @param request request for processing
   * @param params  parameters necessary for connection to the OKAPI
   */
  void process(JsonObject request, JsonObject params, boolean defaultMapping);

}
