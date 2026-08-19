package org.folio.support;

import static org.folio.dataimport.testsupport.postgres.PostgresTestSupport.clearTable;
import static org.folio.dataimport.testsupport.postgres.PostgresTestSupport.getClient;

import io.vertx.core.Future;
import io.vertx.core.Vertx;

public class TestUtil {

  public static final String FILE_EXTENSIONS_TABLE = "file_extensions";
  public static final String UPLOAD_DEFINITIONS_TABLE = "upload_definitions";
  public static final String QUEUE_ITEMS_GLOBAL_SQL = "DELETE FROM data_import_global.queue_items;";

  public static final String TENANT_ID = "diku";
  public static final String MINIO_BUCKET = "test-bucket";
  public static final String SUB_PATH = "mod-data-import";

  public static final String UPLOAD_DEFINITIONS_PATH = "/data-import/uploadDefinitions";
  public static final String FILE_EXTENSIONS_PATH = "/data-import/fileExtensions";
  public static final String ASSEMBLE_PATH =
    "/data-import/uploadDefinitions/{uploadDefinitionId}/files/{fileDefinitionId}/assembleStorageFile";
  public static final String UPLOAD_URL_PATH = "/data-import/uploadUrl";
  public static final String UPLOAD_URL_CONTINUE_PATH = "/data-import/uploadUrl/subsequent";
  public static final String JOB_EXECUTION_CANCEL_PATH =
    "/data-import/jobExecutions/{jobExecutionId}/cancel";
  public static final String DOWNLOAD_URL_PATH =
      "/data-import/jobExecutions/{jobExecutionId}/downloadUrl";
  public static final String DEFINITION_PATH = "/data-import/uploadDefinitions";
  public static final String FILE_EXTENSION_PATH = "/data-import/fileExtensions";
  public static final String DATA_TYPE_PATH = "/data-import/dataTypes";
  public static final String FILE_PATH = "/files";
  public static final String PROCESS_FILE_IMPORT_PATH = "/processFiles";

  public static Future<Void> clearAllTables(Vertx vertx) {
    return clearTable(FILE_EXTENSIONS_TABLE, vertx, TENANT_ID)
      .compose(v -> clearTable(UPLOAD_DEFINITIONS_TABLE, vertx, TENANT_ID))
      .compose(v -> clearGlobalQueueItems(vertx));
  }

  private static Future<Void> clearGlobalQueueItems(Vertx vertx) {
    return getClient(vertx)
      .execute(QUEUE_ITEMS_GLOBAL_SQL)
      .mapEmpty();
  }

  private TestUtil() {
  }
}
