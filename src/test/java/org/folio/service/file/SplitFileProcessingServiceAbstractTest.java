package org.folio.service.file;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.client.HttpResponse;
import java.util.Map;
import org.folio.dao.DataImportQueueItemDao;
import org.folio.rest.AbstractRestTest;
import org.folio.rest.client.ChangeManagerClient;
import org.folio.rest.jaxrs.model.FileDefinition;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.Metadata;
import org.folio.rest.jaxrs.model.ProcessFilesRqDto;
import org.folio.rest.jaxrs.model.UploadDefinition;
import org.folio.service.processing.ParallelFileChunkingProcessor;
import org.folio.service.processing.split.FileSplitService;
import org.folio.service.s3storage.MinioStorageService;
import org.folio.service.upload.UploadDefinitionService;
import org.junit.Before;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(VertxUnitRunner.class)
public abstract class SplitFileProcessingServiceAbstractTest
  extends AbstractRestTest {

  protected static final String PARENT_UPLOAD_DEFINITION_ID =
    "parent-upload-definition-id";
  protected static final UploadDefinition PARENT_UPLOAD_DEFINITION = new UploadDefinition()
    .withId(PARENT_UPLOAD_DEFINITION_ID)
    .withMetadata(null);
  protected static final UploadDefinition PARENT_UPLOAD_DEFINITION_WITH_USER = PARENT_UPLOAD_DEFINITION.withMetadata(
    new Metadata().withCreatedByUserId("user")
  );

  protected static final String PARENT_JOB_EXECUTION_ID =
    "parent-job-execution-id";
  protected static final JobExecution PARENT_JOB_EXECUTION = new JobExecution()
    .withId(PARENT_JOB_EXECUTION_ID);

  protected static final JobProfileInfo JOB_PROFILE_INFO = new JobProfileInfo()
    .withDataType(JobProfileInfo.DataType.MARC);

  protected static final Metadata METADATA = new Metadata()
    .withCreatedByUserId("created-user-id");

  protected static final FileDefinition FILE_DEFINITION_1 = new FileDefinition()
    .withName("file-1")
    .withSourcePath("key/file-1-key");
  protected static final FileDefinition FILE_DEFINITION_2 = new FileDefinition()
    .withName("file-2")
    .withSourcePath("key/file-2-key");
  protected static final FileDefinition FILE_DEFINITION_3 = new FileDefinition()
    .withName("file-3")
    .withSourcePath("key/file-3-key");

  protected static final JobExecution JOB_EXECUTION_1 = new JobExecution()
    .withSourcePath("key/file-1-key");
  protected static final JobExecution JOB_EXECUTION_2 = new JobExecution()
    .withSourcePath("key/file-2-key");
  protected static final JobExecution JOB_EXECUTION_3 = new JobExecution()
    .withSourcePath("key/file-3-key");

  @Mock
  protected FileSplitService fileSplitService;

  @Mock
  protected MinioStorageService minioStorageService;

  @Mock
  protected DataImportQueueItemDao queueItemDao;

  @Mock
  protected UploadDefinitionService uploadDefinitionService;

  @Mock
  protected ParallelFileChunkingProcessor fileProcessor;

  protected ChangeManagerClient changeManagerClient;
  protected SplitFileProcessingServiceProxy service;

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);

    this.changeManagerClient =
      spy(
        new ChangeManagerClient(
          mockServer.baseUrl(),
          AbstractRestTest.TENANT_ID,
          AbstractRestTest.TOKEN
        )
      );

    this.service =
      new SplitFileProcessingServiceProxy(
        vertx,
        fileSplitService,
        minioStorageService,
        queueItemDao,
        uploadDefinitionService,
        fileProcessor
      );
  }

  @SuppressWarnings("unchecked")
  protected AsyncResult<HttpResponse<Buffer>> getSuccessArBuffer(Object obj) {
    HttpResponse<Buffer> response = mock(HttpResponse.class);
    try {
      when(response.bodyAsBuffer())
        .thenReturn(Buffer.buffer(new ObjectMapper().writeValueAsString(obj)));
    } catch (JsonProcessingException e) {
      throw new IllegalStateException(e);
    }
    when(response.statusCode()).thenReturn(200);

    return Future.succeededFuture(response);
  }

  // allow access to protected members for more targeted testing
  protected class SplitFileProcessingServiceProxy
    extends SplitFileProcessingService {

    public SplitFileProcessingServiceProxy(
      Vertx vertx,
      FileSplitService fileSplitService,
      MinioStorageService minioStorageService,
      DataImportQueueItemDao queueItemDao,
      UploadDefinitionService uploadDefinitionService,
      ParallelFileChunkingProcessor fileProcessor
    ) {
      super(
        vertx,
        fileSplitService,
        minioStorageService,
        queueItemDao,
        uploadDefinitionService,
        fileProcessor
      );
    }

    public Future<Map<String, JobExecution>> createParentJobExecutions(
      ProcessFilesRqDto entity,
      ChangeManagerClient client
    ) {
      return super.createParentJobExecutions(entity, client);
    }

    public Future<SplitFileInformation> splitFile(String key) {
      return super.splitFile(key);
    }

    public Buffer verifyOkStatus(HttpResponse<Buffer> response) {
      return super.verifyOkStatus(response);
    }

    public String getUserIdFromMetadata(Metadata metadata) {
      return super.getUserIdFromMetadata(metadata);
    }
  }
}
