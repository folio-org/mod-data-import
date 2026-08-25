package org.folio.service.s3storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;

import org.folio.s3.client.AwsS3Client;
import org.folio.s3.client.MinioS3Client;
import org.folio.s3.client.S3ClientFactory;
import org.folio.s3.client.S3ClientProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class FolioS3ClientFactoryTest {

  @Mock
  private MinioS3Client testMinioClient;

  @Mock
  private AwsS3Client testAwsClient;

  private FolioS3ClientFactory folioS3ClientFactory;

  @BeforeEach
  void setUp() {
    this.folioS3ClientFactory = new FolioS3ClientFactory();
  }

  @DisplayName("should create client on first call and reuse it on subsequent calls")
  @Test
  void shouldCreateClientOnFirstCall_andReuseOnSubsequent() {
    try (MockedStatic<S3ClientFactory> mock = Mockito.mockStatic(S3ClientFactory.class)) {
      mock
        .when(() -> S3ClientFactory.getS3Client(any(S3ClientProperties.class)))
        .thenReturn(testMinioClient, testAwsClient);

      assertThat(folioS3ClientFactory.getFolioS3Client())
        .as("Client is created on first run")
        .isSameAs(testMinioClient);

      assertThat(folioS3ClientFactory.getFolioS3Client())
        .as("Client is not recreated on second run")
        .isSameAs(testMinioClient);

      mock.verify(
        () -> S3ClientFactory.getS3Client(any(S3ClientProperties.class)),
        times(1)
      );
      mock.verifyNoMoreInteractions();
    }
  }
}
