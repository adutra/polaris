/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.polaris.service.storage.s3.sign;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.iceberg.aws.s3.signer.ImmutableS3SignRequest;
import org.apache.iceberg.aws.s3.signer.S3SignRequest;
import org.apache.iceberg.aws.s3.signer.S3SignResponse;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.polaris.service.storage.StorageConfiguration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class S3RequestSignerImplTest {

  @Mock private StorageConfiguration storageConfiguration;
  @Mock private AwsCredentialsProvider awsCredentialsProvider;

  @InjectMocks private S3RequestSignerImpl s3RequestSigner;

  private static final String TEST_ACCESS_KEY = "test-access-key";
  private static final String TEST_SECRET_KEY = "test-secret-key";
  private static final String TEST_REGION = "us-west-2";
  private static final String TEST_HOST = "test-bucket.s3.us-west-2.amazonaws.com";
  private static final URI TEST_URI = URI.create("https://" + TEST_HOST + "/test-path");

  @BeforeEach
  void setUp() {
    AwsCredentials credentials = AwsBasicCredentials.create(TEST_ACCESS_KEY, TEST_SECRET_KEY);
    when(storageConfiguration.awsSystemCredentials())
        .thenReturn(Optional.of(awsCredentialsProvider));
    when(awsCredentialsProvider.resolveCredentials()).thenReturn(credentials);
  }

  @Test
  void testGET() {
    // Given
    S3SignRequest request =
        ImmutableS3SignRequest.builder()
            .region(TEST_REGION)
            .method("GET")
            .uri(TEST_URI)
            .headers(Map.of("Host", List.of(TEST_HOST)))
            .properties(Map.of())
            .build();

    // When
    S3SignResponse response = s3RequestSigner.signRequest(request);

    // Then
    assertThat(response).isNotNull();
    assertThat(response.uri()).isEqualTo(TEST_URI);
    assertHeaders(response);
  }

  @Test
  void testPUT() {
    // Given
    String requestBody = "{\"test\": \"data\"}";
    S3SignRequest request =
        ImmutableS3SignRequest.builder()
            .region(TEST_REGION)
            .method("PUT")
            .uri(TEST_URI)
            .headers(Map.of("Host", List.of(TEST_HOST)))
            .properties(Map.of())
            .body(requestBody)
            .build();

    // When
    S3SignResponse response = s3RequestSigner.signRequest(request);

    // Then
    assertThat(response).isNotNull();
    assertThat(response.uri()).isEqualTo(TEST_URI);
    assertHeaders(response);
  }

  @Test
  void testPOST() {
    // Given
    String requestBody = "{\"test\": \"data\"}";
    S3SignRequest request =
        ImmutableS3SignRequest.builder()
            .region(TEST_REGION)
            .method("POST")
            .uri(TEST_URI)
            .headers(Map.of("Host", List.of(TEST_HOST)))
            .properties(Map.of())
            .body(requestBody)
            .build();

    // When
    S3SignResponse response = s3RequestSigner.signRequest(request);

    // Then
    assertThat(response).isNotNull();
    assertThat(response.uri()).isEqualTo(TEST_URI);
    assertHeaders(response);
  }

  @Test
  void testDELETE() {
    // Given
    String requestBody = "{\"test\": \"data\"}";
    S3SignRequest request =
        ImmutableS3SignRequest.builder()
            .region(TEST_REGION)
            .method("DELETE")
            .uri(TEST_URI)
            .headers(Map.of("Host", List.of(TEST_HOST)))
            .properties(Map.of())
            .body(requestBody)
            .build();

    // When
    S3SignResponse response = s3RequestSigner.signRequest(request);

    // Then
    assertThat(response).isNotNull();
    assertThat(response.uri()).isEqualTo(TEST_URI);
    assertHeaders(response);
  }

  @Test
  void testQueryParameters() {
    // Given
    URI uriWithQuery = URI.create(TEST_URI + "?prefix=test&max-keys=100");
    S3SignRequest request =
        ImmutableS3SignRequest.builder()
            .region(TEST_REGION)
            .method("GET")
            .uri(uriWithQuery)
            .headers(Map.of("Host", List.of(TEST_HOST)))
            .properties(Map.of())
            .build();

    // When
    S3SignResponse response = s3RequestSigner.signRequest(request);

    // Then
    assertThat(response).isNotNull();
    assertThat(response.uri().getHost()).isEqualTo(uriWithQuery.getHost());
    assertThat(response.uri().getPath()).isEqualTo(uriWithQuery.getPath());
    assertThat(response.uri().getQuery()).contains("prefix=test");
    assertThat(response.uri().getQuery()).contains("max-keys=100");
    assertHeaders(response);
  }

  @Test
  void testHeaders() {
    // Given
    S3SignRequest request =
        ImmutableS3SignRequest.builder()
            .region(TEST_REGION)
            .method("GET")
            .uri(TEST_URI)
            .headers(
                Map.of(
                    "Host", List.of(TEST_HOST),
                    "x-amz-content-sha256", List.of("UNSIGNED-PAYLOAD"),
                    "Content-Type", List.of("application/json"),
                    "User-Agent", List.of("test-client/1.0")))
            .properties(Map.of())
            .build();

    // When
    S3SignResponse response = s3RequestSigner.signRequest(request);

    // Then
    assertThat(response).isNotNull();
    assertThat(response.uri()).isEqualTo(TEST_URI);
    assertHeaders(response);
    assertThat(response.headers().get("x-amz-content-sha256").getFirst())
        .isEqualTo("UNSIGNED-PAYLOAD");
    assertThat(response.headers().get("Content-Type").getFirst()).isEqualTo("application/json");
    assertThat(response.headers().get("User-Agent").getFirst()).isEqualTo("test-client/1.0");
  }

  private static void assertHeaders(S3SignResponse response) {
    assertThat(response.headers()).isNotEmpty();
    assertThat(response.headers()).containsKey("X-Amz-Date");
    assertThat(response.headers()).containsKey("x-amz-content-sha256");
    assertThat(response.headers().get("x-amz-content-sha256").getFirst())
        .isEqualTo("UNSIGNED-PAYLOAD"); // Fixed for S3
    assertThat(response.headers()).containsKey("Authorization");
    assertThat(response.headers().get("Authorization")).hasSize(1);
    String authHeader = response.headers().get("Authorization").getFirst();
    assertThat(authHeader).startsWith("AWS4-HMAC-SHA256 Credential=" + TEST_ACCESS_KEY);
    assertThat(authHeader).contains("SignedHeaders=");
    assertThat(authHeader).contains("Signature=");
  }

  static Stream<Arguments> normalizeLocationUriTestCases() {
    return Stream.of(
        // S3 scheme URIs - should return as-is
        Arguments.of("s3://my-bucket/path/to/file", "s3://my-bucket/path/to/file"),
        Arguments.of("s3://my-bucket", "s3://my-bucket"),
        // Virtual-hosted style URLs
        Arguments.of(
            "https://my-bucket.s3.us-west-2.amazonaws.com/path/to/file",
            "s3://my-bucket/path/to/file"),
        Arguments.of(
            "https://my-bucket.s3-us-west-2.amazonaws.com/path/to/file",
            "s3://my-bucket/path/to/file"),
        Arguments.of("https://my-bucket.s3.us-west-2.amazonaws.com/", "s3://my-bucket/"),
        // Path style URLs
        Arguments.of(
            "https://s3.us-west-2.amazonaws.com/my-bucket/path/to/file",
            "s3://my-bucket/path/to/file"),
        Arguments.of(
            "https://s3-us-west-2.amazonaws.com/my-bucket/path/to/file",
            "s3://my-bucket/path/to/file"),
        Arguments.of("https://s3.us-west-2.amazonaws.com/my-bucket", "s3://my-bucket/"),
        Arguments.of("https://s3.us-west-2.amazonaws.com/my-bucket/", "s3://my-bucket/"),
        // Complex paths
        Arguments.of(
            "https://my-bucket.s3.us-east-1.amazonaws.com/data/year=2024/month=01/file.parquet",
            "s3://my-bucket/data/year=2024/month=01/file.parquet"),
        Arguments.of(
            "https://s3.eu-west-1.amazonaws.com/my-bucket/data/year=2024/month=01/file.parquet",
            "s3://my-bucket/data/year=2024/month=01/file.parquet"));
  }

  @ParameterizedTest
  @MethodSource("normalizeLocationUriTestCases")
  void testNormalizeLocationUri(String inputUri, String expectedNormalized) {
    String normalized = s3RequestSigner.normalizeLocationUri(URI.create(inputUri));
    assertThat(normalized).isEqualTo(expectedNormalized);
  }

  @Test
  void testNormalizeLocationUri_InvalidUrl() {
    // Given
    URI invalidUri = URI.create("https://example.com/some/path");

    // When/Then
    assertThatThrownBy(() -> s3RequestSigner.normalizeLocationUri(invalidUri))
        .isInstanceOf(ValidationException.class)
        .hasMessageContaining("Invalid S3 URL");
  }
}
