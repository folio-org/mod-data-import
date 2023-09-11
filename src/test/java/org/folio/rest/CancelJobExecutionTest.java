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
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;
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
            .collect(Collectors.toList())
        )
      );
  }

  @Test
  public void testSuccess(TestContext context) {
    String parentId = "fb1036b0-dd35-4b61-8b64-b041530ba23c";
    String childId1 = "fcdada31-4804-4a61-82c7-0ecb45f91bb2";
    String childId2 = "794a291f-2977-4b9b-a9eb-b03565645440";
    String childId3 = "7bbc916a-d486-4daf-bc6b-6c72648e0b01";
    String childId4 = "a6f70f51-cf03-4879-a926-dab07a7eebf5";

    // set up a few child job executions with parent
    JobExecution childJob1 = new JobExecution()
      .withId(childId1)
      .withParentJobId(parentId);
    JobExecution childJob2 = new JobExecution()
      .withId(childId2)
      .withParentJobId(parentId);
    JobExecution childJob3 = new JobExecution()
      .withId(childId3)
      .withParentJobId(parentId);
    JobExecution childJob4 = new JobExecution()
      .withId(childId4)
      .withParentJobId(parentId);

    List<JobExecutionDto> childList = Arrays.asList(
      new JobExecutionDto().withId(childJob1.getId()),
      new JobExecutionDto().withId(childJob2.getId()),
      new JobExecutionDto().withId(childJob3.getId()),
      new JobExecutionDto().withId(childJob4.getId())
    );

    // set up job execution
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
                new JobExecutionDtoCollection().withJobExecutions(childList)
              )
              .encode()
          )
        )
    );

    CompositeFuture
      .all(
        queueItemDao.addQueueItem(
          new DataImportQueueItem()
            .withId("9eb41611-dad4-45ee-9632-07d0dc2033dd")
            .withJobExecutionId(childId1)
            .withUploadDefinitionId("0bbcd4bd-33b7-4ced-9806-b83f0072797f")
            .withTimestamp(new Date())
        ),
        queueItemDao.addQueueItem(
          new DataImportQueueItem()
            .withId("aa3480d4-f842-4425-a195-a93423803d2b")
            .withJobExecutionId(childId2)
            .withUploadDefinitionId("65bf117b-98ea-44e8-b5a2-c0f925e7989c")
            .withTimestamp(new Date())
        ),
        queueItemDao.addQueueItem(
          new DataImportQueueItem()
            .withId("f29efbb0-d9f7-4f99-a84e-d8901ef4eb0e")
            .withJobExecutionId(childId3)
            .withUploadDefinitionId("eb184d75-188a-4f5b-9f3f-d4735d7748d1")
            .withTimestamp(new Date())
        ),
        queueItemDao.addQueueItem(
          new DataImportQueueItem()
            .withId("b7bdf243-4ff7-430a-85f2-72c3215f1859")
            .withJobExecutionId(childId4)
            .withUploadDefinitionId("62004e64-865c-41c8-8524-a7ddaa367430")
            .withTimestamp(new Date())
        )
      )
      .onComplete(
        context.asyncAssertSuccess(result -> {
          //make request to cancel parent
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
          verify(
            exactly(4),
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
      .withSubordinationType(JobExecution.SubordinationType.PARENT_SINGLE);

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
