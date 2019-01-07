package org.folio.service.processing;

import io.vertx.core.Future;
import org.folio.dataImport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.JobProfile;
import org.folio.rest.jaxrs.model.UploadDefinition;

/**
 * Interface for the file processing runner.
 */
public interface FileProcessingRunner {

  /**
   * Runs file processing for given UploadDefinition entity.
   *
   * @param uploadDefinition UploadDefinition entity which files will be handled
   * @param jobProfile       JobProfile chosen by the user
   * @param params           parameters necessary to connect to the OKAPI
   * @return Future
   */
  Future<Void> run(UploadDefinition uploadDefinition, JobProfile jobProfile, OkapiConnectionParams params);
}
