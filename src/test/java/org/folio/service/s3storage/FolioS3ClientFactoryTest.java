package org.folio.service.s3storage;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;

import org.folio.s3.client.AwsS3Client;
import org.folio.s3.client.MinioS3Client;
import org.folio.s3.client.S3ClientFactory;
import org.folio.s3.client.S3ClientProperties;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

public class FolioS3ClientFactoryTest {

  @Mock
  private MinioS3Client testMinioClient;

  @Mock
  private AwsS3Client testAwsClient;

  @InjectMocks
  private FolioS3ClientFactory folioS3ClientFactory;

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  public void testFolioS3ClientCreation() {
    MockedStatic<S3ClientFactory> mock = Mockito.mockStatic(
      S3ClientFactory.class
    );

    mock
      .when(() -> S3ClientFactory.getS3Client(any(S3ClientProperties.class)))
      .thenReturn(testMinioClient, testAwsClient);

    assertEquals(
      "Client is created on first run",
      testMinioClient,
      folioS3ClientFactory.getFolioS3Client()
    );

    assertEquals(
      "Client is not recreated on second run",
      testMinioClient,
      folioS3ClientFactory.getFolioS3Client()
    );

    mock.verify(
      () -> S3ClientFactory.getS3Client(any(S3ClientProperties.class)),
      times(1)
    );
    mock.verifyNoMoreInteractions();
  }
}
