package org.folio.service;

import java.util.HashMap;
import java.util.Map;

public class FileStorageServiceRepository {

  private Map<String, FileStorageService> services = new HashMap<>();

  public FileStorageService getService(String name) {
    return services.get(name);
  }

  public void setServices(String name, FileStorageService service) {
    services.put(name, service);
  }
}
