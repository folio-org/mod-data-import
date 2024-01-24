package org.folio.service.kafka;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.admin.KafkaAdminClient;
import io.vertx.kafka.admin.NewTopic;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.kafka.common.errors.TopicExistsException;
import org.folio.kafka.services.KafkaAdminClientService;
import org.folio.rest.impl.util.DataImportKafkaTopic;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;

@RunWith(VertxUnitRunner.class)
public class KafkaAdminClientServiceTest {

  private final String STUB_TENANT = "foo-tenant";
  private KafkaAdminClient mockClient;
  private Vertx vertx;

  @Before
  public void setUp() {
    vertx = mock(Vertx.class);
    mockClient = mock(KafkaAdminClient.class);
  }

  @Test
  public void shouldCreateTopicIfAlreadyExist(TestContext testContext) {
    when(mockClient.createTopics(anyList()))
      .thenReturn(failedFuture(new TopicExistsException("x")))
      .thenReturn(failedFuture(new TopicExistsException("y")))
      .thenReturn(failedFuture(new TopicExistsException("z")))
      .thenReturn(succeededFuture());
    when(mockClient.listTopics()).thenReturn(succeededFuture(Set.of("old")));
    when(mockClient.close()).thenReturn(succeededFuture());

    createKafkaTopicsAsync(mockClient)
      .onComplete(testContext.asyncAssertSuccess(notUsed -> {
        verify(mockClient, times(4)).listTopics();
        verify(mockClient, times(4)).createTopics(anyList());
        verify(mockClient, times(1)).close();
      }));
  }

  @Test
  public void shouldFailIfExistExceptionIsPermanent(TestContext testContext) {
    when(mockClient.createTopics(anyList())).thenReturn(failedFuture(new TopicExistsException("x")));
    when(mockClient.listTopics()).thenReturn(succeededFuture(Set.of("old")));
    when(mockClient.close()).thenReturn(succeededFuture());

    createKafkaTopicsAsync(mockClient)
      .onComplete(testContext.asyncAssertFailure(e -> {
        assertThat(e, instanceOf(TopicExistsException.class));
        verify(mockClient, times(1)).close();
      }));
  }

  @Test
  public void shouldNotCreateTopicOnOther(TestContext testContext) {
    when(mockClient.createTopics(anyList())).thenReturn(failedFuture(new RuntimeException("err msg")));
    when(mockClient.listTopics()).thenReturn(succeededFuture(Set.of("old")));
    when(mockClient.close()).thenReturn(succeededFuture());

    createKafkaTopicsAsync(mockClient)
      .onComplete(testContext.asyncAssertFailure(cause -> {
          testContext.assertEquals("err msg", cause.getMessage());
          verify(mockClient, times(1)).close();
        }
      ));
  }

  @Test
  public void shouldCreateTopicIfNotExist(TestContext testContext) {
    when(mockClient.createTopics(anyList())).thenReturn(succeededFuture());
    when(mockClient.listTopics()).thenReturn(succeededFuture(Set.of("old")));
    when(mockClient.close()).thenReturn(succeededFuture());

    createKafkaTopicsAsync(mockClient)
      .onComplete(testContext.asyncAssertSuccess(notUsed -> {

        @SuppressWarnings("unchecked")
        final ArgumentCaptor<List<NewTopic>> createTopicsCaptor = forClass(List.class);

        verify(mockClient, times(1)).createTopics(createTopicsCaptor.capture());
        verify(mockClient, times(1)).close();

        // Only these items are expected, so implicitly checks size of list
        assertThat(getTopicNames(createTopicsCaptor), containsInAnyOrder(allExpectedTopics.toArray()));
      }));
  }

  private List<String> getTopicNames(ArgumentCaptor<List<NewTopic>> createTopicsCaptor) {
    return createTopicsCaptor.getAllValues().get(0).stream()
      .map(NewTopic::getName)
      .collect(Collectors.toList());
  }

  private Future<Void> createKafkaTopicsAsync(KafkaAdminClient client) {
    try (var mocked = mockStatic(KafkaAdminClient.class)) {
      mocked.when(() -> KafkaAdminClient.create(eq(vertx), anyMap())).thenReturn(client);

      return new KafkaAdminClientService(vertx)
        .createKafkaTopics(DataImportKafkaTopic.values(), STUB_TENANT);
    }
  }

  private final Set<String> allExpectedTopics = Set.of("folio.foo-tenant.data-import.di-completed",
    "folio.foo-tenant.data-import.di-edifact-record-created",
    "folio.foo-tenant.data-import.di-error",
    "folio.foo-tenant.data-import.di-initialization-started",
    "folio.foo-tenant.data-import.di-inventory-authority-created-ready-for-post-processing",
    "folio.foo-tenant.data-import.di-inventory-instance-created-ready-for-post-processing",
    "folio.foo-tenant.data-import.di-log-srs-marc-authority-record-created",
    "folio.foo-tenant.data-import.di-log-srs-marc-bib-record-created",
    "folio.foo-tenant.data-import.di-marc-bib-for-order-created",
    "folio.foo-tenant.data-import.di-marc-for-update-received",
    "folio.foo-tenant.data-import.di-parsed-records-chunk-saved",
    "folio.foo-tenant.data-import.di-raw-records-chunk-parsed",
    "folio.foo-tenant.data-import.di-raw-records-chunk-read",
    "folio.foo-tenant.data-import.di-srs-marc-authority-record-created",
    "folio.foo-tenant.data-import.di-srs-marc-authority-record-not-matched",
    "folio.foo-tenant.data-import.di-srs-marc-bib-instance-hrid-set",
    "folio.foo-tenant.data-import.di-srs-marc-bib-record-created",
    "folio.foo-tenant.data-import.di-srs-marc-bib-record-modified-ready-for-post-processing"
  );
}
