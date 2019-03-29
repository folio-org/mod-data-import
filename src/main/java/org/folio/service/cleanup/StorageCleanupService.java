package org.folio.service.cleanup;

import io.vertx.core.Future;
import org.folio.dataimport.util.OkapiConnectionParams;

public interface StorageCleanupService {

  /**
   * Cleans storage from files linked to completed uploadDefinition or which have not been updated for {@code N} ms.
   * Value for {@code N} can be recieved from mod-config, in case of receiving error {@code N = 3600000} ms.
   *
   * @param params Okapi connection params
   * @return Future with true if files were deleted and false in otherwise.
   */
  Future<Boolean> cleanStorage(OkapiConnectionParams params);
}
