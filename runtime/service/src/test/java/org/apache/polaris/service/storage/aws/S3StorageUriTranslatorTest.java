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

package org.apache.polaris.service.storage.aws;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.net.URI;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class S3StorageUriTranslatorTest {

  @ParameterizedTest
  @MethodSource("translateTestCases")
  void testTranslate(URI inputUri, String expected) {
    String normalized = new S3StorageUriTranslator().translate(inputUri, null);
    assertThat(normalized).isEqualTo(expected);
  }

  static Stream<Arguments> translateTestCases() {
    return Stream.of(
        // ===== Virtual Hosted Style =====

        // Virtual hosted style - US and China
        Arguments.of("http://my-bucket.s3.region-name-1.amazonaws.com", "s3://my-bucket"),
        Arguments.of("HTTP://MY-BUCKET.S3.REGION-NAME-1.AMAZONAWS.COM", "s3://my-bucket"),
        Arguments.of("http://my-bucket.s3.region-name-1.amazonaws.com/", "s3://my-bucket/"),
        Arguments.of("https://my-bucket.s3.region-name-1.amazonaws.com", "s3://my-bucket"),
        Arguments.of("HTTPS://MY-BUCKET.S3.REGION-NAME-1.AMAZONAWS.COM", "s3://my-bucket"),
        Arguments.of("https://my-bucket.s3.region-name-1.amazonaws.com/", "s3://my-bucket/"),
        Arguments.of("https://my-bucket.s3.region-name-1.amazonaws.com.cn", "s3://my-bucket"),
        Arguments.of("https://my-bucket.s3.region-name-1.amazonaws.com.cn/", "s3://my-bucket/"),
        Arguments.of(
            "https://my-bucket.s3.region-name-1.amazonaws.com/path/to/file",
            "s3://my-bucket/path/to/file"),
        Arguments.of(
            "https://my-bucket.s3.region-name-1.amazonaws.com.cn/path/to/file",
            "s3://my-bucket/path/to/file"),

        // Virtual hosted style - FIPS, Accelerate, Dualstack
        Arguments.of(
            "https://my-bucket.s3-fips.region-name-1.amazonaws.com/path/to/file",
            "s3://my-bucket/path/to/file"),
        Arguments.of(
            "https://my-bucket.s3-accelerate.region-name-1.amazonaws.com/path/to/file",
            "s3://my-bucket/path/to/file"),
        Arguments.of(
            "https://my-bucket.s3.dualstack.region-name-1.amazonaws.com/path/to/file",
            "s3://my-bucket/path/to/file"),
        Arguments.of(
            "https://my-bucket.s3-fips.dualstack.region-name-1.amazonaws.com/path/to/file",
            "s3://my-bucket/path/to/file"),
        Arguments.of(
            "https://my-bucket.s3-accelerate.dualstack.region-name-1.amazonaws.com/path/to/file",
            "s3://my-bucket/path/to/file"),

        // Virtual hosted style - Access Point
        // always virtual hosted style, the bucket name is replaced by the access point name
        Arguments.of(
            "https://my-access-point.s3-accesspoint.region-name-1.amazonaws.com/path/to/file",
            "s3://my-access-point/path/to/file"),
        Arguments.of(
            "https://my-access-point.s3-accesspoint-fips.region-name-1.amazonaws.com/path/to/file",
            "s3://my-access-point/path/to/file"),
        Arguments.of(
            "https://my-access-point.s3-accesspoint.dualstack.region-name-1.amazonaws.com/path/to/file",
            "s3://my-access-point/path/to/file"),
        Arguments.of(
            "https://my-access-point.s3-accesspoint-fips.dualstack.region-name-1.amazonaws.com/path/to/file",
            "s3://my-access-point/path/to/file"),

        // ===== Path Style =====

        // Path style - US and China
        Arguments.of("https://s3.region-name-1.amazonaws.com/my-bucket", "s3://my-bucket"),
        Arguments.of("https://s3.region-name-1.amazonaws.com/my-bucket/", "s3://my-bucket/"),
        Arguments.of("https://s3.region-name-1.amazonaws.com.cn/my-bucket", "s3://my-bucket"),
        Arguments.of("https://s3.region-name-1.amazonaws.com.cn/my-bucket/", "s3://my-bucket/"),
        Arguments.of(
            "https://s3.region-name-1.amazonaws.com/my-bucket/path/to/file",
            "s3://my-bucket/path/to/file"),
        Arguments.of(
            "https://s3.region-name-1.amazonaws.com.cn/my-bucket/path/to/file",
            "s3://my-bucket/path/to/file"),

        // Path style - Legacy S3 global URLs
        Arguments.of("https://s3.amazonaws.com/my-bucket", "s3://my-bucket"),
        Arguments.of("https://s3.amazonaws.com/my-bucket/", "s3://my-bucket/"),
        Arguments.of(
            "https://s3.amazonaws.com/my-bucket/path/to/file", "s3://my-bucket/path/to/file"),

        // Path style - FIPS, Accelerate, Dualstack
        Arguments.of(
            "https://s3-fips.region-name-1.amazonaws.com/my-bucket/path/to/file",
            "s3://my-bucket/path/to/file"),
        Arguments.of(
            "https://s3-accelerate.region-name-1.amazonaws.com/my-bucket/path/to/file",
            "s3://my-bucket/path/to/file"),
        Arguments.of(
            "https://s3.dualstack.region-name-1.amazonaws.com/my-bucket/path/to/file",
            "s3://my-bucket/path/to/file"),
        Arguments.of(
            "https://s3-fips.dualstack.region-name-1.amazonaws.com/my-bucket/path/to/file",
            "s3://my-bucket/path/to/file"),
        Arguments.of(
            "https://s3-accelerate.dualstack.region-name-1.amazonaws.com/my-bucket/path/to/file",
            "s3://my-bucket/path/to/file"),

        // ===== Corner Cases =====

        // Buckets with dots (possible in path style only)
        Arguments.of(
            "https://s3.region-name-1.amazonaws.com/my.app.bucket/data/year=2024/month=01/file.parquet",
            "s3://my.app.bucket/data/year=2024/month=01/file.parquet"),
        // Keys with non-ASCII characters
        Arguments.of(
            "https://mybucket.s3.region-name-1.amazonaws.com.cn/照片/image.jpg",
            "s3://mybucket/照片/image.jpg"),
        // Keys with URL encoded characters
        Arguments.of(
            "https://mybucket.s3.region-name-1.amazonaws.com/%E7%85%A7%E7%89%87/image.jpg",
            "s3://mybucket/照片/image.jpg"));
  }

  @ParameterizedTest
  @MethodSource("translateWithEndpointTestCases")
  void testTranslateWithEndpoint(URI inputUri, URI endpoint, String expected) {
    String normalized = new S3StorageUriTranslator().translate(inputUri, endpoint);
    assertThat(normalized).isEqualTo(expected);
  }

  static Stream<Arguments> translateWithEndpointTestCases() {
    return Stream.of(

        // ===== MinIO Virtual Hosted Style =====

        Arguments.of(
            "http://my-bucket.minio.local:9000",
            URI.create("http://minio.local:9000"),
            "s3://my-bucket"),
        Arguments.of(
            "http://my-bucket.minio.local:9000/",
            URI.create("http://minio.local:9000"),
            "s3://my-bucket/"),
        Arguments.of(
            "http://my-bucket.minio.local:9000/path/to/file",
            URI.create("http://minio.local:9000"),
            "s3://my-bucket/path/to/file"),

        // ===== MinIO Path Style =====

        // Basic path style with MinIO endpoint
        Arguments.of(
            "http://localhost:9000/my-bucket",
            URI.create("http://localhost:9000"),
            "s3://my-bucket"),
        Arguments.of(
            "http://localhost:9000/my-bucket/",
            URI.create("http://localhost:9000/"),
            "s3://my-bucket/"),
        Arguments.of(
            "http://localhost/my-bucket/path/to/file",
            URI.create("http://localhost"),
            "s3://my-bucket/path/to/file"),

        // ===== Edge Cases =====

        // Bucket with dots
        Arguments.of(
            "http://localhost:9000/my.app.bucket/data/file.parquet",
            URI.create("http://localhost:9000"),
            "s3://my.app.bucket/data/file.parquet"),
        Arguments.of(
            "http://my.app.bucket.minio.local:9000/data/file.parquet",
            URI.create("http://minio.local:9000"),
            "s3://my.app.bucket/data/file.parquet"));
  }

  @ParameterizedTest
  @MethodSource("translateFailureTestCases")
  void testTranslateInvalid(URI invalidUri, String expectedMessage) {
    assertThatThrownBy(() -> new S3StorageUriTranslator().translate(invalidUri, null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(expectedMessage);
  }

  static Stream<Arguments> translateFailureTestCases() {
    return Stream.of(
        Arguments.of("/some/path", "Invalid S3 URL: /some/path"),
        Arguments.of(
            "arn:aws:s3:::my-bucket/path/to/file",
            "Invalid S3 URL: arn:aws:s3:::my-bucket/path/to/file"),
        Arguments.of("https:///some/path", "Invalid S3 URL: https:///some/path"),
        Arguments.of(
            "https://other-service.amazonaws.com/some/path",
            "Invalid S3 URL: https://other-service.amazonaws.com/some/path"),
        Arguments.of("https://s3.amazonaws.com/", "Invalid S3 URL: https://s3.amazonaws.com/"),
        Arguments.of("https://s3.amazonaws.com", "Invalid S3 URL: https://s3.amazonaws.com"),
        Arguments.of(
            "https://non-s3-host.com/some/path",
            "Invalid S3 URL: https://non-s3-host.com/some/path"));
  }

  @ParameterizedTest
  @MethodSource("translateWithEndpointFailureTestCases")
  void testTranslateWithEndpointInvalid(URI invalidUri, URI endpoint, String expectedMessage) {
    assertThatThrownBy(() -> new S3StorageUriTranslator().translate(invalidUri, endpoint))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(expectedMessage);
  }

  static Stream<Arguments> translateWithEndpointFailureTestCases() {
    return Stream.of(
        // Host doesn't match endpoint
        Arguments.of(
            "http://other-host:9000/bucket/file",
            URI.create("http://localhost:9000"),
            "Invalid S3 URL: http://other-host:9000/bucket/file"),
        // Path style with no bucket
        Arguments.of(
            "http://localhost:9000/",
            URI.create("http://localhost:9000"),
            "Invalid S3 URL: http://localhost:9000/"));
  }
}
