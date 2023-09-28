package org.folio.rest;

import static com.github.tomakehurst.wiremock.client.WireMock.exactly;
import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.notFound;
import static com.github.tomakehurst.wiremock.client.WireMock.okJson;
import static com.github.tomakehurst.wiremock.client.WireMock.putRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.urlPathMatching;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static org.hamcrest.Matchers.is;

import com.github.tomakehurst.wiremock.client.WireMock;
import io.restassured.RestAssured;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import org.apache.http.HttpStatus;
import org.folio.dao.DataImportQueueItemDao;
import org.folio.dao.DataImportQueueItemDaoImpl;
import org.folio.dao.util.PostgresClientFactory;
import org.folio.rest.jaxrs.model.DataImportQueueItem;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobExecutionDto;
import org.folio.rest.jaxrs.model.JobExecutionDtoCollection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockitoAnnotations;

@RunWith(VertxUnitRunner.class)
public class CancelJobExecutionTest extends AbstractRestTest {

  private static final String JOB_EXECUTION_CANCEL_PATH =
    "/data-import/jobExecutions/{jobExecutionId}/cancel";

  DataImportQueueItemDao queueItemDao;

  @Before
  public void setUp() throws IOException {
    MockitoAnnotations.openMocks(this);
    queueItemDao = new DataImportQueueItemDaoImpl(new PostgresClientFactory());
  }

  @After
  public void cleanUp(TestContext context) {
    queueItemDao
      .getAllQueueItems()
      .compose(items ->
        CompositeFuture.all(
          items
            .getDataImportQueueItems()
            .stream()
            .map(DataImportQueueItem::getId)
            .map(queueItemDao::deleteDataImportQueueItem)
            .map(Future.class::cast)
            .toList()
        )
      )
      .onComplete(context.asyncAssertSuccess());
  }

  @Test
  public void testSuccess(TestContext context) {
    String parentId = "fb1036b0-dd35-4b61-8b64-b041530ba23c";

    // test different states
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

    JobExecutionDto newJob = new JobExecutionDto()
      .withId(newId)
      .withParentJobId(parentId)
      .withStatus(JobExecutionDto.Status.NEW);
    JobExecutionDto parsingInProgressJob = new JobExecutionDto()
      .withId(parsingInProgressId)
      .withParentJobId(parentId)
      .withStatus(JobExecutionDto.Status.PARSING_IN_PROGRESS);
    JobExecutionDto parsingFinishedJob = new JobExecutionDto()
      .withId(parsingFinishedId)
      .withParentJobId(parentId)
      .withStatus(JobExecutionDto.Status.PARSING_FINISHED);
    JobExecutionDto processingInProgressJob = new JobExecutionDto()
      .withId(processingInProgressId)
      .withParentJobId(parentId)
      .withStatus(JobExecutionDto.Status.PROCESSING_IN_PROGRESS);
    JobExecutionDto processingFinishedJob = new JobExecutionDto()
      .withId(processingFinishedId)
      .withParentJobId(parentId)
      .withStatus(JobExecutionDto.Status.PROCESSING_FINISHED);
    JobExecutionDto commitInProgressJob = new JobExecutionDto()
      .withId(commitInProgressId)
      .withParentJobId(parentId)
      .withStatus(JobExecutionDto.Status.COMMIT_IN_PROGRESS);
    JobExecutionDto committedJob = new JobExecutionDto()
      .withId(committedId)
      .withParentJobId(parentId)
      .withStatus(JobExecutionDto.Status.COMMITTED);
    JobExecutionDto errorJob = new JobExecutionDto()
      .withId(errorId)
      .withParentJobId(parentId)
      .withStatus(JobExecutionDto.Status.ERROR);
    JobExecutionDto discardedJob = new JobExecutionDto()
      .withId(discardedId)
      .withParentJobId(parentId)
      .withStatus(JobExecutionDto.Status.DISCARDED);
    JobExecutionDto cancelledJob = new JobExecutionDto()
      .withId(cancelledId)
      .withParentJobId(parentId)
      .withStatus(JobExecutionDto.Status.CANCELLED);

    // set up parent job execution
    JobExecution cancelJob = new JobExecution()
      .withId(parentId)
      .withSubordinationType(JobExecution.SubordinationType.COMPOSITE_PARENT);

    // mock response to parent
    WireMock.stubFor(
      get(urlPathMatching("/change-manager/jobExecutions/" + parentId))
        .willReturn(okJson(JsonObject.mapFrom(cancelJob).encode()))
    );

    // mock response to children
    WireMock.stubFor(
      get(
        urlPathMatching(
          "/change-manager/jobExecutions/" + parentId + "/children"
        )
      )
        .willReturn(
          okJson(
            JsonObject
              .mapFrom(
                new JobExecutionDtoCollection()
                  .withJobExecutions(
                    Arrays.asList(
                      newJob,
                      parsingInProgressJob,
                      parsingFinishedJob,
                      processingInProgressJob,
                      processingFinishedJob,
                      commitInProgressJob,
                      committedJob,
                      errorJob,
                      discardedJob,
                      cancelledJob
                    )
                  )
              )
              .encode()
          )
        )
    );

    // entirely arbitrary which ones we choose here; just want to have some to reference
    // when cancelling child jobs, it will delete from the queue if present, but it does not
    // use that as an indicator of if it's processed/etc.
    CompositeFuture
      .all(
        queueItemDao.addQueueItem(
          new DataImportQueueItem()
            .withId("9eb41611-dad4-45ee-9632-07d0dc2033dd")
            .withJobExecutionId(newJob.getId())
            .withUploadDefinitionId("0bbcd4bd-33b7-4ced-9806-b83f0072797f")
            .withTimestamp(new Date())
        ),
        queueItemDao.addQueueItem(
          new DataImportQueueItem()
            .withId("aa3480d4-f842-4425-a195-a93423803d2b")
            .withJobExecutionId(parsingInProgressJob.getId())
            .withUploadDefinitionId("65bf117b-98ea-44e8-b5a2-c0f925e7989c")
            .withTimestamp(new Date())
        ),
        queueItemDao.addQueueItem(
          new DataImportQueueItem()
            .withId("f29efbb0-d9f7-4f99-a84e-d8901ef4eb0e")
            .withJobExecutionId(parsingFinishedJob.getId())
            .withUploadDefinitionId("eb184d75-188a-4f5b-9f3f-d4735d7748d1")
            .withTimestamp(new Date())
        ),
        queueItemDao.addQueueItem(
          new DataImportQueueItem()
            .withId("b7bdf243-4ff7-430a-85f2-72c3215f1859")
            .withJobExecutionId(processingInProgressJob.getId())
            .withUploadDefinitionId("62004e64-865c-41c8-8524-a7ddaa367430")
            .withTimestamp(new Date())
        )
      )
      .onComplete(
        context.asyncAssertSuccess(result -> {
          // make request to cancel parent
          RestAssured
            .given()
            .spec(spec)
            .pathParam("jobExecutionId", parentId)
            .when()
            .delete(JOB_EXECUTION_CANCEL_PATH)
            .then()
            .log()
            .all()
            .statusCode(HttpStatus.SC_OK)
            .body("ok", is(true));

          queueItemDao
            .getAllQueueItems()
            .onComplete(
              context.asyncAssertSuccess(items -> {
                context.assertEquals(0, items.getDataImportQueueItems().size());
              })
            );

          verify(
            exactly(2),
            getRequestedFor(urlPathMatching("/change-manager/jobExecutions/.*"))
          );
          verify(
            exactly(1),
            getRequestedFor(
              urlPathMatching("/change-manager/jobExecutions/.*/children")
            )
          );

          // verify all the actual cancels
          Arrays
            .asList(
              parentId,
              newId,
              parsingInProgressId,
              parsingFinishedId,
              processingInProgressId,
              processingFinishedId,
              commitInProgressId
            )
            .forEach(id -> {
              verify(
                exactly(1),
                putRequestedFor(
                  urlPathMatching(
                    "/change-manager/jobExecutions/" + id + "/status"
                  )
                )
              );
            });

          // verify no more than those verified above
          verify(
            exactly(7),
            putRequestedFor(
              urlPathMatching("/change-manager/jobExecutions/.*/status")
            )
          );
        })
      );
  }

  @Test
  public void testNonParent(TestContext context) {
    String parentId = "fb1036b0-dd35-4b61-8b64-b041530ba23c";

    // set up job execution
    JobExecution job = new JobExecution()
      .withId(parentId)
      .withSubordinationType(JobExecution.SubordinationType.COMPOSITE_CHILD);

    // mock response to parent
    WireMock.stubFor(
      get(urlPathMatching("/change-manager/jobExecutions/" + parentId))
        .willReturn(okJson(JsonObject.mapFrom(job).encode()))
    );

    // make request to cancel parent
    RestAssured
      .given()
      .spec(spec)
      .pathParam("jobExecutionId", parentId)
      .when()
      .delete(JOB_EXECUTION_CANCEL_PATH)
      .then()
      .log()
      .all()
      .statusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
  }

  @Test
  public void testNotFound(TestContext context) {
    String parentId = "fb1036b0-dd35-4b61-8b64-b041530ba23c";

    // mock response to parent
    WireMock.stubFor(
      get(urlPathMatching("/change-manager/jobExecutions/" + parentId))
        .willReturn(notFound())
    );

    // make request to cancel parent
    RestAssured
      .given()
      .spec(spec)
      .pathParam("jobExecutionId", parentId)
      .when()
      .delete(JOB_EXECUTION_CANCEL_PATH)
      .then()
      .log()
      .all()
      .statusCode(HttpStatus.SC_INTERNAL_SERVER_ERROR);
  }
}
