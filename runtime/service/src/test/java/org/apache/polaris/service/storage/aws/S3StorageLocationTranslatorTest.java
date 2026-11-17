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
import org.apache.polaris.core.storage.StorageLocation;
import org.apache.polaris.core.storage.aws.S3Location;
import org.apache.polaris.service.storage.aws.S3StorageLocationTranslator.S3Context;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class S3StorageLocationTranslatorTest {

  private final S3StorageLocationTranslator translator = new S3StorageLocationTranslator();

  @ParameterizedTest
  @MethodSource("translateTestCases")
  void testTranslate(URI inputUri, boolean pathStyleAccess, URI endpoint, String expected) {
    var context = new S3StorageLocationTranslator.S3Context(pathStyleAccess, endpoint);
    StorageLocation translated = translator.translate(inputUri, context);
    assertThat(translated).isInstanceOf(S3Location.class);
    assertThat(translated.toString()).isEqualTo(expected);
  }

  static Stream<Arguments> translateTestCases() {
    return Stream.of(
        // ===== Virtual Hosted Style =====

        // Virtual hosted style - US and China
        Arguments.of(
            "http://my-bucket.s3.region-name-1.amazonaws.com", false, null, "s3://my-bucket"),
        Arguments.of(
            "HTTP://MY-BUCKET.S3.REGION-NAME-1.AMAZONAWS.COM", false, null, "s3://my-bucket"),
        Arguments.of(
            "http://my-bucket.s3.region-name-1.amazonaws.com/", false, null, "s3://my-bucket/"),
        Arguments.of(
            "https://my-bucket.s3.region-name-1.amazonaws.com", false, null, "s3://my-bucket"),
        Arguments.of(
            "HTTPS://MY-BUCKET.S3.REGION-NAME-1.AMAZONAWS.COM", false, null, "s3://my-bucket"),
        Arguments.of(
            "https://my-bucket.s3.region-name-1.amazonaws.com/", false, null, "s3://my-bucket/"),
        Arguments.of(
            "https://my-bucket.s3.region-name-1.amazonaws.com.cn", false, null, "s3://my-bucket"),
        Arguments.of(
            "https://my-bucket.s3.region-name-1.amazonaws.com.cn/", false, null, "s3://my-bucket/"),
        Arguments.of(
            "https://my-bucket.s3.region-name-1.amazonaws.com/path/to/file",
            false,
            null,
            "s3://my-bucket/path/to/file"),
        Arguments.of(
            "https://my-bucket.s3.region-name-1.amazonaws.com.cn/path/to/file",
            false,
            null,
            "s3://my-bucket/path/to/file"),

        // Virtual hosted style - FIPS, Accelerate, Dualstack
        Arguments.of(
            "https://my-bucket.s3-fips.region-name-1.amazonaws.com/path/to/file",
            false,
            null,
            "s3://my-bucket/path/to/file"),
        Arguments.of(
            "https://my-bucket.s3-accelerate.region-name-1.amazonaws.com/path/to/file",
            false,
            null,
            "s3://my-bucket/path/to/file"),
        Arguments.of(
            "https://my-bucket.s3.dualstack.region-name-1.amazonaws.com/path/to/file",
            false,
            null,
            "s3://my-bucket/path/to/file"),
        Arguments.of(
            "https://my-bucket.s3-fips.dualstack.region-name-1.amazonaws.com/path/to/file",
            false,
            null,
            "s3://my-bucket/path/to/file"),
        Arguments.of(
            "https://my-bucket.s3-accelerate.dualstack.region-name-1.amazonaws.com/path/to/file",
            false,
            null,
            "s3://my-bucket/path/to/file"),

        // Virtual hosted style - Access Point
        // always virtual hosted style, the bucket name is replaced by the access point name
        Arguments.of(
            "https://my-access-point.s3-accesspoint.region-name-1.amazonaws.com/path/to/file",
            false,
            null,
            "s3://my-access-point/path/to/file"),
        Arguments.of(
            "https://my-access-point.s3-accesspoint-fips.region-name-1.amazonaws.com/path/to/file",
            false,
            null,
            "s3://my-access-point/path/to/file"),
        Arguments.of(
            "https://my-access-point.s3-accesspoint.dualstack.region-name-1.amazonaws.com/path/to/file",
            false,
            null,
            "s3://my-access-point/path/to/file"),
        Arguments.of(
            "https://my-access-point.s3-accesspoint-fips.dualstack.region-name-1.amazonaws.com/path/to/file",
            false,
            null,
            "s3://my-access-point/path/to/file"),

        // Virtual hosted style - S3-like
        Arguments.of("http://my-bucket.minio.local", false, "http://minio.local", "s3://my-bucket"),
        Arguments.of(
            "http://my-bucket.minio.local/", false, "http://minio.local", "s3://my-bucket/"),
        Arguments.of(
            "http://my-bucket.minio.local/path/to/file",
            false,
            "http://minio.local",
            "s3://my-bucket/path/to/file"),

        // ===== Path Style =====

        // Path style - US and China
        Arguments.of(
            "https://s3.region-name-1.amazonaws.com/my-bucket", true, null, "s3://my-bucket"),
        Arguments.of(
            "https://s3.region-name-1.amazonaws.com/my-bucket/", true, null, "s3://my-bucket/"),
        Arguments.of(
            "https://s3.region-name-1.amazonaws.com.cn/my-bucket", true, null, "s3://my-bucket"),
        Arguments.of(
            "https://s3.region-name-1.amazonaws.com.cn/my-bucket/", true, null, "s3://my-bucket/"),
        Arguments.of(
            "https://s3.region-name-1.amazonaws.com/my-bucket/path/to/file",
            true,
            null,
            "s3://my-bucket/path/to/file"),
        Arguments.of(
            "https://s3.region-name-1.amazonaws.com.cn/my-bucket/path/to/file",
            true,
            null,
            "s3://my-bucket/path/to/file"),

        // Path style - Legacy S3 global URLs
        Arguments.of("https://s3.amazonaws.com/my-bucket", true, null, "s3://my-bucket"),
        Arguments.of("https://s3.amazonaws.com/my-bucket/", true, null, "s3://my-bucket/"),
        Arguments.of(
            "https://s3.amazonaws.com/my-bucket/path/to/file",
            true,
            null,
            "s3://my-bucket/path/to/file"),

        // Path style - FIPS, Accelerate, Dualstack
        Arguments.of(
            "https://s3-fips.region-name-1.amazonaws.com/my-bucket/path/to/file",
            true,
            null,
            "s3://my-bucket/path/to/file"),
        Arguments.of(
            "https://s3-accelerate.region-name-1.amazonaws.com/my-bucket/path/to/file",
            true,
            null,
            "s3://my-bucket/path/to/file"),
        Arguments.of(
            "https://s3.dualstack.region-name-1.amazonaws.com/my-bucket/path/to/file",
            true,
            null,
            "s3://my-bucket/path/to/file"),
        Arguments.of(
            "https://s3-fips.dualstack.region-name-1.amazonaws.com/my-bucket/path/to/file",
            true,
            null,
            "s3://my-bucket/path/to/file"),
        Arguments.of(
            "https://s3-accelerate.dualstack.region-name-1.amazonaws.com/my-bucket/path/to/file",
            true,
            null,
            "s3://my-bucket/path/to/file"),

        // Path style - S3-like
        Arguments.of(
            "http://minio.example.com/my-bucket",
            true,
            "http://minio.example.com",
            "s3://my-bucket"),
        Arguments.of(
            "http://minio.example.com/my-bucket/",
            true,
            "http://minio.example.com",
            "s3://my-bucket/"),
        Arguments.of(
            "http://minio.example.com/my-bucket/path/to/file",
            true,
            "http://minio.example.com",
            "s3://my-bucket/path/to/file"),
        Arguments.of(
            "http://localhost:9000/my-bucket", true, "http://localhost:9000", "s3://my-bucket"),
        Arguments.of(
            "http://localhost:9000/my-bucket/", true, "http://localhost:9000", "s3://my-bucket/"),
        Arguments.of(
            "http://localhost:9000/my-bucket/path/to/file",
            true,
            "http://localhost:9000",
            "s3://my-bucket/path/to/file"),

        // ===== Corner Cases =====

        // Buckets with dots
        Arguments.of(
            "https://my.app.bucket.s3.region-name-1.amazonaws.com/data/year=2024/month=01/file.parquet",
            false,
            null,
            "s3://my.app.bucket/data/year=2024/month=01/file.parquet"),
        Arguments.of(
            "https://s3.region-name-1.amazonaws.com/my.app.bucket/data/year=2024/month=01/file.parquet",
            true,
            null,
            "s3://my.app.bucket/data/year=2024/month=01/file.parquet"),
        Arguments.of(
            "http://minio.local:9000/my.app.bucket/data/file.parquet",
            true,
            "http://minio.local:9000",
            "s3://my.app.bucket/data/file.parquet"),
        Arguments.of(
            "http://localhost:9000/my.app.bucket/data/file.parquet",
            true,
            "http://localhost:9000",
            "s3://my.app.bucket/data/file.parquet"),

        // Explicit AWS endpoint
        Arguments.of(
            "https://my-bucket.s3.region-name-1.amazonaws.com/path/to/file",
            false,
            "https://s3.region-name-1.amazonaws.com",
            "s3://my-bucket/path/to/file"),
        Arguments.of(
            "https://s3.region-name-1.amazonaws.com/my-bucket/path/to/file",
            true,
            "https://s3.region-name-1.amazonaws.com",
            "s3://my-bucket/path/to/file"),

        // Buckets with dots, virtual hosted style, custom endpoint
        Arguments.of(
            "http://my.app.bucket.minio.local:9000/data/file.parquet",
            false,
            "http://minio.local:9000",
            "s3://my.app.bucket/data/file.parquet"),

        // Default ports
        Arguments.of(
            "https://minio.local:443/my-bucket/file",
            true,
            "https://minio.local",
            "s3://my-bucket/file"),

        // Keys with non-ASCII characters
        Arguments.of(
            "https://mybucket.s3.region-name-1.amazonaws.com.cn/照片/image.jpg",
            false,
            null,
            "s3://mybucket/照片/image.jpg"),

        // Keys with URL encoded characters
        Arguments.of(
            "https://mybucket.s3.region-name-1.amazonaws.com/%E7%85%A7%E7%89%87/image.jpg",
            false, null, "s3://mybucket/照片/image.jpg"));
  }

  @ParameterizedTest
  @MethodSource("translateFailureTestCases")
  void testTranslateInvalid(
      URI invalidUri, boolean pathStyleAccess, URI endpoint, String expectedMessage) {
    S3Context context = new S3Context(pathStyleAccess, endpoint);
    assertThatThrownBy(() -> new S3StorageLocationTranslator().translate(invalidUri, context))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage(expectedMessage);
  }

  static Stream<Arguments> translateFailureTestCases() {
    return Stream.of(
        // missing scheme
        Arguments.of("/some/path", false, null, "Invalid S3 URL: /some/path"),
        // wrong scheme
        Arguments.of(
            "arn:aws:s3:::my-bucket/path/to/file",
            false,
            null,
            "Invalid S3 URL: arn:aws:s3:::my-bucket/path/to/file"),
        // missing host
        Arguments.of("https:///some/path", false, null, "Invalid S3 URL: https:///some/path"),
        // missing bucket in path style
        Arguments.of(
            "https://s3.amazonaws.com", true, null, "Invalid S3 URL: https://s3.amazonaws.com"),
        Arguments.of(
            "https://s3.amazonaws.com/", true, null, "Invalid S3 URL: https://s3.amazonaws.com/"),
        Arguments.of(
            "https://s3.amazonaws.com//path",
            true,
            null,
            "Invalid S3 URL: https://s3.amazonaws.com//path"),
        // missing bucket in virtual hosted style
        Arguments.of(
            "https://localhost/some/path",
            false,
            null,
            "Invalid S3 URL: https://localhost/some/path"),
        Arguments.of(
            "https://.minio.local/some/path",
            false,
            "https://minio.local",
            "Invalid S3 URL: https://.minio.local/some/path"),
        Arguments.of(
            "https://.s3.region-name-1.amazonaws.com/some/path",
            false,
            null,
            "Invalid S3 URL: https://.s3.region-name-1.amazonaws.com/some/path"),
        // virtual hosted style URL with path style access
        Arguments.of(
            "https://other-bucket.s3.region-name-1.amazonaws.com/my-bucket/path",
            true,
            null,
            "Invalid S3 URL: https://other-bucket.s3.region-name-1.amazonaws.com/my-bucket/path"),
        Arguments.of(
            "https://s3-bucket.s3.region-name-1.amazonaws.com/my-bucket/path",
            true,
            null,
            "Invalid S3 URL: https://s3-bucket.s3.region-name-1.amazonaws.com/my-bucket/path"),
        Arguments.of(
            "http://other-bucket.minio.local:9000/my-bucket/path",
            true,
            "http://minio.local:9000",
            "Invalid S3 URL: http://other-bucket.minio.local:9000/my-bucket/path"),
        // host not matching the endpoint
        Arguments.of(
            "http://other.local:9000/my-bucket/path",
            true,
            "http://minio.local:9000",
            "Invalid S3 URL: http://other.local:9000/my-bucket/path"),
        Arguments.of(
            "http://minio.local:9001/my-bucket/path",
            true,
            "http://minio.local:9000",
            "Invalid S3 URL: http://minio.local:9001/my-bucket/path"),
        Arguments.of(
            "http://my-bucket.other.local:9000/path",
            false,
            "http://minio.local:9000",
            "Invalid S3 URL: http://my-bucket.other.local:9000/path"),
        Arguments.of(
            "http://my-bucket.minio.local:9001/path",
            false,
            "http://minio.local:9000",
            "Invalid S3 URL: http://my-bucket.minio.local:9001/path"),
        Arguments.of(
            "http://minio.local:9000/my-bucket/path",
            false,
            "http://minio.local:9000",
            "Invalid S3 URL: http://minio.local:9000/my-bucket/path"),
        // non-AWS host without endpoint
        Arguments.of(
            "http://minio.example.com/my-bucket",
            true,
            null,
            "Invalid S3 URL: http://minio.example.com/my-bucket"),
        Arguments.of(
            "http://my-bucket.minio.local/path",
            false,
            null,
            "Invalid S3 URL: http://my-bucket.minio.local/path"));
  }
}
