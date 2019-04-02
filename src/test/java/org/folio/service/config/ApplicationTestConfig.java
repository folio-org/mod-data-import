package org.folio.service.config;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages = {
  "org.folio.dao",
  "org.folio.service.cleanup",
  "org.folio.service.storage"})
public class ApplicationTestConfig {}
