package org.folio.service.kafka;

import static io.vertx.core.Future.failedFuture;
import static io.vertx.core.Future.succeededFuture;
import static org.assertj.core.api.Assertions.assertThat;
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
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.kafka.admin.KafkaAdminClient;
import io.vertx.kafka.admin.NewTopic;
import java.util.List;
import java.util.Set;
import org.apache.kafka.common.errors.TopicExistsException;
import org.folio.kafka.services.KafkaAdminClientService;
import org.folio.kafka.services.KafkaEnvironmentProperties;
import org.folio.kafka.services.KafkaTopic;
import org.folio.service.kafka.support.DataImportKafkaTopic;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
class KafkaAdminClientServiceTest {

  private static final String STUB_TENANT = "foo-tenant";
  private static final String ENV_VARIABLE = "folio";
  private final Set<String> allExpectedTopics = Set.of(
    "folio.Default.foo-tenant.DI_INITIALIZATION_STARTED",
    "folio.Default.foo-tenant.DI_RAW_RECORDS_CHUNK_READ"
  );
  private KafkaAdminClient mockClient;
  private Vertx vertx;
  @Mock
  private KafkaTopicConfiguration kafkaTopicConfiguration;

  @BeforeEach
  void setUp() {
    vertx = mock(Vertx.class);
    mockClient = mock(KafkaAdminClient.class);

    KafkaTopic[] topicObjects = {
      new DataImportKafkaTopic("DI_INITIALIZATION_STARTED", 10),
      new DataImportKafkaTopic("DI_RAW_RECORDS_CHUNK_READ", 10),
      };
    when(kafkaTopicConfiguration.createTopicObjects()).thenReturn(topicObjects);
  }

  @Test
  @DisplayName("should retry until topics are created when TopicExistsException is transient")
  void shouldRetry_untilTopicsCreated_whenTopicExistsExceptionIsTransient(VertxTestContext testContext) {
    when(mockClient.createTopics(anyList()))
      .thenReturn(failedFuture(new TopicExistsException("x")))
      .thenReturn(failedFuture(new TopicExistsException("y")))
      .thenReturn(failedFuture(new TopicExistsException("z")))
      .thenReturn(succeededFuture());
    when(mockClient.listTopics()).thenReturn(succeededFuture(Set.of("old")));
    when(mockClient.close()).thenReturn(succeededFuture());

    createKafkaTopicsAsync(mockClient)
      .onComplete(
        testContext.succeeding(notUsed ->
          testContext.verify(() -> {
            verify(mockClient, times(4)).listTopics();
            verify(mockClient, times(4)).createTopics(anyList());
            verify(mockClient, times(1)).close();
            testContext.completeNow();
          })
        )
      );
  }

  @Test
  @DisplayName("should fail when TopicExistsException is permanent")
  void shouldFail_whenTopicExistsExceptionIsPermanent(VertxTestContext testContext) {
    when(mockClient.createTopics(anyList()))
      .thenReturn(failedFuture(new TopicExistsException("x")));
    when(mockClient.listTopics()).thenReturn(succeededFuture(Set.of("old")));
    when(mockClient.close()).thenReturn(succeededFuture());

    createKafkaTopicsAsync(mockClient)
      .onComplete(
        testContext.failing(e ->
          testContext.verify(() -> {
            assertThat(e).isInstanceOf(TopicExistsException.class);
            verify(mockClient, times(1)).close();
            testContext.completeNow();
          })
        )
      );
  }

  @Test
  @DisplayName("should fail immediately when a non-TopicExistsException occurs")
  void shouldFailImmediately_whenNonTopicExistsExceptionOccurs(VertxTestContext testContext) {
    when(mockClient.createTopics(anyList()))
      .thenReturn(failedFuture(new RuntimeException("err msg")));
    when(mockClient.listTopics()).thenReturn(succeededFuture(Set.of("old")));
    when(mockClient.close()).thenReturn(succeededFuture());

    createKafkaTopicsAsync(mockClient)
      .onComplete(
        testContext.failing(cause ->
          testContext.verify(() -> {
            assertThat(cause.getMessage()).isEqualTo("err msg");
            verify(mockClient, times(1)).close();
            testContext.completeNow();
          })
        )
      );
  }

  @Test
  @DisplayName("should create topics with correct names when topics do not exist")
  void shouldCreateTopicsWithCorrectNames_whenTopicsDoNotExist(VertxTestContext testContext) {
    when(mockClient.createTopics(anyList())).thenReturn(succeededFuture());
    when(mockClient.listTopics()).thenReturn(succeededFuture(Set.of("old")));
    when(mockClient.close()).thenReturn(succeededFuture());

    createKafkaTopicsAsync(mockClient)
      .onComplete(
        testContext.succeeding(notUsed ->
          testContext.verify(() -> {
            @SuppressWarnings("unchecked") final ArgumentCaptor<List<NewTopic>> createTopicsCaptor =
              forClass(List.class);

            verify(mockClient, times(1)).createTopics(createTopicsCaptor.capture());
            verify(mockClient, times(1)).close();

            assertThat(getTopicNames(createTopicsCaptor))
              .containsExactlyInAnyOrderElementsOf(allExpectedTopics);

            testContext.completeNow();
          })
        )
      );
  }

  private List<String> getTopicNames(ArgumentCaptor<List<NewTopic>> createTopicsCaptor) {
    return createTopicsCaptor
      .getAllValues()
      .getFirst()
      .stream()
      .map(NewTopic::getName)
      .toList();
  }

  private Future<Void> createKafkaTopicsAsync(KafkaAdminClient client) {
    try (
      MockedStatic<KafkaAdminClient> mockedClient = mockStatic(KafkaAdminClient.class);
      MockedStatic<KafkaEnvironmentProperties> mockedEnv = mockStatic(
        KafkaEnvironmentProperties.class
      )
    ) {
      mockedClient
        .when(() -> KafkaAdminClient.create(eq(vertx), anyMap()))
        .thenReturn(client);
      mockedEnv
        .when(KafkaEnvironmentProperties::environment)
        .thenReturn(ENV_VARIABLE);

      return new KafkaAdminClientService(vertx)
        .createKafkaTopics(kafkaTopicConfiguration.createTopicObjects(), STUB_TENANT);
    }
  }
}
