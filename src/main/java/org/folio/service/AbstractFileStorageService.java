package org.folio.service;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.FileSystem;
import io.vertx.core.json.JsonObject;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.folio.rest.client.ConfigurationsClient;
import org.folio.rest.jaxrs.model.FileDefinition;

import javax.ws.rs.BadRequestException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class AbstractFileStorageService implements FileStorageService {

  private static final String OKAPI_URL_HEADER = "X-Okapi-URL";
  private static final String OKAPI_TENANT_HEADER = "X-Okapi-Tenant";
  private static final String OKAPI_TOKEN_HEADER = "X-Okapi-Token";
  private static final Pattern HOST_PORT_PATTERN = Pattern.compile("https?://([^:/]+)(?::?(\\d+)?)");
  private static final int DEFAULT_PORT = 9030;

  private Vertx vertx;
  private String tenantId;
  private FileSystem fs;

  public AbstractFileStorageService(Vertx vertx, String tenantId) {
    this.vertx = vertx;
    this.tenantId = tenantId;
    this.fs = vertx.fileSystem();
  }

  @Override
  public String getServiceName() {
    return "LOCAL_STORAGE";
  }


  @Override
  public Future<Buffer> getFile(String path) {
    return null;
  }

  @Override
  public Future<FileDefinition> saveFile(InputStream data, FileDefinition fileDefinition) {
    Future<FileDefinition> future = Future.future();
    getStoragePath(fileDefinition.getUploadDefinitionId())
      .setHandler(pathReply -> {
        if (pathReply.succeeded()) {
          try {
            String path = pathReply.result();
            fs.mkdirsBlocking(path);
            fs.writeFileBlocking(path + "/" + fileDefinition.getName(), Buffer.buffer(IOUtils.toByteArray(data)));
            fileDefinition.setSourcePath(path + "/" + fileDefinition.getName());
            fileDefinition.setLoaded(true);
            future.complete(fileDefinition);
          } catch (Exception e) {
            future.fail(e);
          }
        } else {
          future.fail(new BadRequestException());
        }
      });
    return future;
  }

  public Future<String> getStoragePath(String uploadDefinitionId) {
    return Future.succeededFuture("./storage/upload/" + uploadDefinitionId);
  }

  /**
   * Maps aliases to configuration parameters
   * @param fieldAliasList - a list of aliases
   * @return a list of user fields to use for search
   */
  private io.vertx.core.Future<List<String>> getLocateUserFields(List<String> fieldAliasList, java.util.Map<String, String> okapiHeaders) {
    //TODO:

    String okapiURL = okapiHeaders.get(OKAPI_URL_HEADER);
    String tenant = okapiHeaders.get(OKAPI_TENANT_HEADER);
    String token = okapiHeaders.get(OKAPI_TOKEN_HEADER);

    Matcher matcher = HOST_PORT_PATTERN.matcher(okapiURL);
    if (!matcher.find()) {
      return io.vertx.core.Future.failedFuture("Could not parse okapiURL: " + okapiURL);
    }
    io.vertx.core.Future<List<String>> future = io.vertx.core.Future.future();

    String host = matcher.group(1);
    String port = matcher.group(2);

    ConfigurationsClient configurationsClient = new ConfigurationsClient(host, StringUtils.isNotBlank(port) ? Integer.valueOf(port) : DEFAULT_PORT, tenant, token);
    StringBuilder query = new StringBuilder("module==DATA_IMPORT AND (")
      .append(fieldAliasList.stream()
        .map(f -> "code==\"" + f + "\"")
        .collect(Collectors.joining(" or ")))
      .append(")");

    try {
      configurationsClient.getEntries(query.toString(), 0, 3, null, null, response ->
        response.bodyHandler(body -> {
          if (response.statusCode() != 200) {
            future.fail("Expected status code 200, got '" + response.statusCode() +
              "' :" + body.toString());
            return;
          }
          JsonObject entries = body.toJsonObject();

          future.complete(
            entries.getJsonArray("configs").stream()
              .map(o -> ((JsonObject) o).getString("value"))
              .flatMap(s -> Stream.of(s.split("[^\\w.]+")))
              .collect(Collectors.toList()));
        })
      );
    } catch (UnsupportedEncodingException e) {
      future.fail(e);
    }
    return future;
  }

  @Override
  public Future<Boolean> deleteFile(String path) {
    return null;
  }

}
