package org.folio.service.chunking;

import io.vertx.core.Future;
import org.folio.rest.jaxrs.model.UploadDefinition;
import org.folio.util.OkapiConnectionParams;

/**
 * Interface for the file chunking handler.
 */
public interface FileChunkingHandler {

  /**
   * Handles files linked to given UploadDefinition entity.
   * Stating initiation of the file dividing process for given UploadDefinition entity.
   * Divides all the files linked to one UploadDefinition into chunks of data
   * and sends it the mod-source-record-manager.
   *
   * @param uploadDefinition UploadDefinition entity which files will be handled
   * @param profile          choosen JobExecutionProfile by the user
   * @param params           parameters necessary to connect to the OKAPI
   * @return Future parametrized by handled {@link UploadDefinition}
   */
  Future<UploadDefinition> handle(UploadDefinition uploadDefinition, JobExecutionProfile profile, OkapiConnectionParams params);
}
