/*
 * Copyright 2022 The Data Transfer Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.datatransferproject.transfer.common;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.atLeast;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import org.datatransferproject.api.launcher.Monitor;
import org.datatransferproject.transfer.exception.ICPSRCredentialsException;
import org.datatransferproject.transfer.icpsr.common.ICPSRDataTransferClient;
import org.datatransferproject.transfer.icpsr.common.ICPSRS3ClientFactory;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.Bucket;
import software.amazon.awssdk.services.s3.model.BucketAlreadyExistsException;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CompleteMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadRequest;
import software.amazon.awssdk.services.s3.model.CreateMultipartUploadResponse;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.UploadPartRequest;
import software.amazon.awssdk.services.s3.model.UploadPartResponse;

@ExtendWith(MockitoExtension.class)
public class ICPSRDataTransferClientTest {

  @Mock
  private Monitor monitor;
  @Mock
  private ICPSRS3ClientFactory icpsrS3ClientFactory;
  @Mock
  private S3Client s3Client;
  private static File testFile;
  private static final String KEY_ID = "keyId";
  private static final String APP_KEY = "appKey";
  private static final String EXPORT_SERVICE = "exp-serv";
  private static final String FILE_KEY = "fileKey";
  private static final String VALID_BUCKET_NAME = EXPORT_SERVICE + "-data-transfer-bucket";

  @BeforeAll
  public static void setUpClass() {
    testFile = new File("src/test/resources/test.txt");
  }

  @BeforeEach
  public void setUp() {
    lenient().when(icpsrS3ClientFactory.createS3Client(anyString(), anyString(), anyString()))
        .thenReturn(s3Client);
  }

  private void createValidBucketList() {
    Bucket bucket = Bucket.builder().name(VALID_BUCKET_NAME).build();
    when(s3Client.listBuckets()).thenReturn(ListBucketsResponse.builder().buckets(bucket).build());
  }

  private void createEmptyBucketList() {
    when(s3Client.listBuckets()).thenReturn(ListBucketsResponse.builder().build());
  }

  private ICPSRDataTransferClient createDefaultClient() {
    return new ICPSRDataTransferClient(monitor, icpsrS3ClientFactory, 1000, 500);
  }

  @Test
  public void testWrongPartSize() {
    assertThrows(IllegalArgumentException.class, () -> {
      new ICPSRDataTransferClient(monitor, icpsrS3ClientFactory, 10, 0);
    });
  }

  @Test
  public void testInitBucketNameMatches() throws ICPSRCredentialsException, IOException {
    createValidBucketList();
    ICPSRDataTransferClient client = createDefaultClient();
    client.init(KEY_ID, APP_KEY, EXPORT_SERVICE);
    verify(s3Client, times(0)).createBucket(any(CreateBucketRequest.class));
  }

  @Test
  public void testInitBucketCreated() throws ICPSRCredentialsException, IOException {
    Bucket bucket = Bucket.builder().name("invalid-name").build();
    when(s3Client.listBuckets()).thenReturn(ListBucketsResponse.builder().buckets(bucket).build());
    ICPSRDataTransferClient client = createDefaultClient();
    client.init(KEY_ID, APP_KEY, EXPORT_SERVICE);
    verify(s3Client, times(1)).createBucket(any(CreateBucketRequest.class));
  }

  @Test
  public void testInitBucketNameExists() throws ICPSRCredentialsException, IOException {
    createEmptyBucketList();
    when(s3Client.createBucket(any(CreateBucketRequest.class)))
        .thenThrow(BucketAlreadyExistsException.builder().build());
    ICPSRDataTransferClient client = createDefaultClient();

    assertThrows(IOException.class, () -> {
      client.init(KEY_ID, APP_KEY, EXPORT_SERVICE);
    });
    verify(monitor, atLeast(1)).info(any());
  }

  @Test
  public void testInitErrorCreatingBucket() throws ICPSRCredentialsException, IOException {
    createEmptyBucketList();
    when(s3Client.createBucket(any(CreateBucketRequest.class)))
        .thenThrow(AwsServiceException.builder().build());
    ICPSRDataTransferClient client = createDefaultClient();
    assertThrows(IOException.class, () -> {
      client.init(KEY_ID, APP_KEY, EXPORT_SERVICE);
    });
  }

  @Test
  public void testInitListBucketException() throws ICPSRCredentialsException, IOException {
    when(s3Client.listBuckets()).thenThrow(S3Exception.builder().statusCode(403).build());
    ICPSRDataTransferClient client = createDefaultClient();
    assertThrows(ICPSRCredentialsException.class, () -> {
      client.init(KEY_ID, APP_KEY, EXPORT_SERVICE);
    });
    verify(s3Client, atLeast(1)).close();
    verify(monitor, atLeast(1)).debug(any());
  }

  @Test
  public void testUploadFileNonInitialized() throws IOException {
    ICPSRDataTransferClient client = createDefaultClient();
    assertThrows(IllegalStateException.class, () -> {
      client.uploadFile(FILE_KEY, testFile);
    });
  }

  @Test
  public void testUploadFileSingle() throws ICPSRCredentialsException, IOException {
    final String expectedVersionId = "123";
    createValidBucketList();
    when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
        .thenReturn(PutObjectResponse.builder().versionId(expectedVersionId).build());
    ICPSRDataTransferClient client = createDefaultClient();
    client.init(KEY_ID, APP_KEY, EXPORT_SERVICE);
    String actualVersionId = client.uploadFile(FILE_KEY, testFile);
    verify(s3Client, times(1)).putObject(any(PutObjectRequest.class), any(RequestBody.class));
    assertEquals(expectedVersionId, actualVersionId);
  }

  @Test
  public void testUploadFileSingleException() throws ICPSRCredentialsException, IOException {
    createValidBucketList();
    when(s3Client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
        .thenThrow(AwsServiceException.builder().build());
    ICPSRDataTransferClient client = createDefaultClient();
    client.init(KEY_ID, APP_KEY, EXPORT_SERVICE);
    assertThrows(IOException.class, () -> {
      client.uploadFile(FILE_KEY, testFile);
    });
  }

  @Test
  public void testUploadFileMultipart() throws ICPSRCredentialsException, IOException {
    final String expectedVersionId = "123";
    createValidBucketList();
    when(s3Client.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
        .thenReturn(CreateMultipartUploadResponse.builder().uploadId("xyz").build());
    when(s3Client.uploadPart(any(UploadPartRequest.class), any(RequestBody.class)))
        .thenReturn(UploadPartResponse.builder().build());
    when(s3Client.completeMultipartUpload(any(CompleteMultipartUploadRequest.class)))
        .thenReturn(CompleteMultipartUploadResponse.builder().versionId(expectedVersionId).build());
    final long partSize = 10;
    final long fileSize = testFile.length();
    final long expectedParts = fileSize / partSize + (fileSize % partSize == 0 ? 0 : 1);
    ICPSRDataTransferClient client =
        new ICPSRDataTransferClient(monitor, icpsrS3ClientFactory, fileSize / 2, partSize);
    client.init(KEY_ID, APP_KEY, EXPORT_SERVICE);
    String actualVersionId = client.uploadFile(FILE_KEY, testFile);
    verify(s3Client, times((int) expectedParts))
        .uploadPart(any(UploadPartRequest.class), any(RequestBody.class));
    assertEquals(expectedVersionId, actualVersionId);
  }

  @Test
  public void testUploadFileMultipartException() throws ICPSRCredentialsException, IOException {
    createValidBucketList();
    when(s3Client.createMultipartUpload(any(CreateMultipartUploadRequest.class)))
        .thenReturn(CreateMultipartUploadResponse.builder().uploadId("xyz").build());
    when(s3Client.uploadPart(any(UploadPartRequest.class), any(RequestBody.class)))
        .thenThrow(AwsServiceException.builder().build());
    final long fileSize = testFile.length();
    ICPSRDataTransferClient client =
        new ICPSRDataTransferClient(monitor, icpsrS3ClientFactory, fileSize / 2,
            fileSize / 8);
    client.init(KEY_ID, APP_KEY, EXPORT_SERVICE);
    assertThrows(IOException.class, () -> {
      client.uploadFile(FILE_KEY, testFile);
    });
  }
}
