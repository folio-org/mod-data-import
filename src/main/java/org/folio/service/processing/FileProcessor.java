package org.folio.service.processing;

import io.vertx.core.Future;
import org.folio.dataImport.util.OkapiConnectionParams;
import org.folio.rest.jaxrs.model.UploadDefinition;

/**
 * Processing files associated with given upload definition.
 */
public interface FileProcessor {

  /**
   * Performs processing files related to given upload definition
   *
   * @param uploadDefinition target upload definition entity
   * @param params           parameters necessary for connection to the OKAPI
   * @return Future
   */
  Future<Void> process(UploadDefinition uploadDefinition, OkapiConnectionParams params);

}
