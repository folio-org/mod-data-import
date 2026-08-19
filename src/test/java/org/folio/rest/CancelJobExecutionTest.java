package org.folio.rest;

import static com.github.tomakehurst.wiremock.client.WireMock.deleteRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.exactly;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.notFound;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static org.assertj.core.api.Assertions.assertThat;
import static org.folio.support.TestUtil.JOB_EXECUTION_CANCEL_PATH;
import static org.hamcrest.Matchers.is;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import java.util.Arrays;
import java.util.Date;
import org.apache.http.HttpStatus;
import org.folio.dao.DataImportQueueItemDao;
import org.folio.dao.DataImportQueueItemDaoImpl;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.dataimport.testsupport.vertx.VertxTestUtil;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionDto;
import org.folio.rest.jaxrs.model.JobExecutionDtoCollection;
import org.folio.support.AbstractRestTest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

class CancelJobExecutionTest extends AbstractRestTest {

  private DataImportQueueItemDao queueItemDao;

  @BeforeEach
  void setUpDao() {
    queueItemDao = new DataImportQueueItemDaoImpl(new PostgresClientFactory(vertx));
  }

  @DisplayName("should cancel job execution and clean queue when parent is COMPOSITE_PARENT")
  @Test
  void shouldCancelJobExecutionAndCleanQueue_whenParentIsCompositeParent() {
    String parentId = "fb1036b0-dd35-4b61-8b64-b041530ba23c";
    String newId = "2a1279bb-cbac-5940-8f4d-e49ff41b3fd3";
    String parsingInProgressId = "13d3a5fa-efe6-5319-80e1-eb43dc2b8fb5";
    String parsingFinishedId = "d89fb7a5-6c9b-5bde-b76a-f4871ab4a541";
    String processingInProgressId = "bdb500bb-abb4-5c15-bbf7-9c995f532cbd";
    String processingFinishedId = "199f2e6c-06f4-5331-ad06-b4d0163b211d";
    String commitInProgressId = "d7e14feb-3065-590e-b1dc-90985098c7a1";
    String committedId = "c470797a-6a49-5e63-bc2c-cd2562e3c065";
    String errorId = "f1e9782d-2afd-58c9-b152-f659d522a45c";
    String discardedId = "aebc9987-c533-55db-8649-5999f36fc43c";
    String cancelledId = "3b42b27c-e624-5f53-bbb5-13c079c266c7";

    JobExecution cancelJob = new JobExecution()
      .withId(parentId)
      .withSubordinationType(JobExecution.SubordinationType.COMPOSITE_PARENT);

    WIRE_MOCK.stubFor(get(urlPathMatching("/change-manager/jobExecutions/" + parentId))
      .willReturn(okJson(JsonObject.mapFrom(cancelJob).encode())));

    WIRE_MOCK.stubFor(get(urlPathMatching("/change-manager/jobExecutions/" + parentId + "/children"))
      .willReturn(okJson(JsonObject.mapFrom(new JobExecutionDtoCollection().withJobExecutions(Arrays.asList(
        new JobExecutionDto().withId(newId).withParentJobId(parentId).withStatus(JobExecutionDto.Status.NEW),
        new JobExecutionDto().withId(parsingInProgressId).withParentJobId(parentId)
          .withStatus(JobExecutionDto.Status.PARSING_IN_PROGRESS),
        new JobExecutionDto().withId(parsingFinishedId).withParentJobId(parentId)
          .withStatus(JobExecutionDto.Status.PARSING_FINISHED),
        new JobExecutionDto().withId(processingInProgressId).withParentJobId(parentId)
          .withStatus(JobExecutionDto.Status.PROCESSING_IN_PROGRESS),
        new JobExecutionDto().withId(processingFinishedId).withParentJobId(parentId)
          .withStatus(JobExecutionDto.Status.PROCESSING_FINISHED),
        new JobExecutionDto().withId(commitInProgressId).withParentJobId(parentId)
          .withStatus(JobExecutionDto.Status.COMMIT_IN_PROGRESS),
        new JobExecutionDto().withId(committedId).withParentJobId(parentId)
          .withStatus(JobExecutionDto.Status.COMMITTED),
        new JobExecutionDto().withId(errorId).withParentJobId(parentId).withStatus(JobExecutionDto.Status.ERROR),
        new JobExecutionDto().withId(discardedId).withParentJobId(parentId)
          .withStatus(JobExecutionDto.Status.DISCARDED),
        new JobExecutionDto().withId(cancelledId).withParentJobId(parentId).withStatus(JobExecutionDto.Status.CANCELLED)
      ))).encode())));

    VertxTestUtil.await(Future.all(
      queueItemDao.addQueueItem(new DataImportQueueItem()
        .withId("9eb41611-dad4-45ee-9632-07d0dc2033dd").withJobExecutionId(newId)
        .withUploadDefinitionId("0bbcd4bd-33b7-4ced-9806-b83f0072797f").withTimestamp(new Date())),
      queueItemDao.addQueueItem(new DataImportQueueItem()
        .withId("aa3480d4-f842-4425-a195-a93423803d2b").withJobExecutionId(parsingInProgressId)
        .withUploadDefinitionId("65bf117b-98ea-44e8-b5a2-c0f925e7989c").withTimestamp(new Date())),
      queueItemDao.addQueueItem(new DataImportQueueItem()
        .withId("f29efbb0-d9f7-4f99-a84e-d8901ef4eb0e").withJobExecutionId(parsingFinishedId)
        .withUploadDefinitionId("eb184d75-188a-4f5b-9f3f-d4735d7748d1").withTimestamp(new Date())),
      queueItemDao.addQueueItem(new DataImportQueueItem()
        .withId("b7bdf243-4ff7-430a-85f2-72c3215f1859").withJobExecutionId(processingInProgressId)
        .withUploadDefinitionId("62004e64-865c-41c8-8524-a7ddaa367430").withTimestamp(new Date()))
    ));

    given()
      .pathParam("jobExecutionId", parentId)
      .delete(JOB_EXECUTION_CANCEL_PATH)
      .then().log().all()
      .statusCode(HttpStatus.SC_OK)
      .body("ok", is(true));


    WIRE_MOCK.verify(exactly(2), getRequestedFor(urlPathMatching("/change-manager/jobExecutions/.*")));
    WIRE_MOCK.verify(exactly(1), getRequestedFor(urlPathMatching("/change-manager/jobExecutions/.*/children")));

    Arrays.asList(parentId, newId, parsingInProgressId, parsingFinishedId,
        processingInProgressId, processingFinishedId, commitInProgressId)
      .forEach(id -> WIRE_MOCK.verify(exactly(1),
        deleteRequestedFor(urlPathMatching("/change-manager/jobExecutions/" + id + "/records"))));

    WIRE_MOCK.verify(exactly(7), deleteRequestedFor(urlPathMatching("/change-manager/jobExecutions/.*/records")));

    var items = VertxTestUtil.await(queueItemDao.getAllQueueItems());
    assertThat(items.getDataImportQueueItems()).isEmpty();
  }

  @DisplayName("should return 500 when job execution is not a composite parent")
  @Test
  void shouldReturn500_whenJobExecutionIsNotCompositeParent() {
    String parentId = "fb1036b0-dd35-4b61-8b64-b041530ba23c";

    WIRE_MOCK.stubFor(get(urlPathMatching("/change-manager/jobExecutions/" + parentId))
      .willReturn(okJson(JsonObject.mapFrom(new JobExecution()
        .withId(parentId)
        .withSubordinationType(JobExecution.SubordinationType.COMPOSITE_CHILD)).encode())));

    given()
      .pathParam("jobExecutionId", parentId)
      .delete(JOB_EXECUTION_CANCEL_PATH)
      .then().log().all()
      .statusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
  }

  @DisplayName("should return 500 when job execution is not found")
  @Test
  void shouldReturn500_whenJobExecutionNotFound() {
    String parentId = "fb1036b0-dd35-4b61-8b64-b041530ba23c";

    WIRE_MOCK.stubFor(get(urlPathMatching("/change-manager/jobExecutions/" + parentId))
      .willReturn(notFound()));

    given()
      .pathParam("jobExecutionId", parentId)
      .delete(JOB_EXECUTION_CANCEL_PATH)
      .then().log().all()
      .statusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
  }
}
