package org.folio.config;

import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

@Configuration
@ComponentScan(basePackages = {
  "org.folio.rest.impl",
  "org.folio.dao",
  "org.folio.service.file",
  "org.folio.service.fileextension",
  "org.folio.service.upload"})
public class ApplicationConfig {}
