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
import java.util.List;
import java.util.Map;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.rest.requests.ImmutableRemoteSignRequest;
import org.apache.iceberg.rest.requests.RemoteSignRequest;
import org.apache.polaris.core.storage.StorageLocation;
import org.apache.polaris.service.storage.StorageLocationTranslator.RequestTarget;
import org.apache.polaris.service.storage.aws.S3StorageLocationTranslator.S3Context;
import org.jspecify.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class S3SignRequestPolicyTest {

  private static final String BUCKET_URL = "https://bucket.s3.us-west-2.amazonaws.com";
  private static final S3Context CONTEXT = new S3Context(false, null);

  private final S3StorageLocationTranslator translator = new S3StorageLocationTranslator();

  private static RemoteSignRequest request(
      String method, String url, Map<String, List<String>> headers, @Nullable String body) {
    return ImmutableRemoteSignRequest.builder()
        .method(method)
        .uri(URI.create(url))
        .region("us-west-2")
        .headers(headers)
        .body(body)
        .build();
  }

  private static RemoteSignRequest request(String method, String url) {
    return request(method, url, Map.of(), null);
  }

  private static String deleteObjectsBody(String... objects) {
    return "<?xml version=\"1.0\" encoding=\"UTF-8\"?>"
        + "<Delete xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
        + String.join("", objects)
        + "<Quiet>true</Quiet></Delete>";
  }

  private RequestTarget target(RemoteSignRequest request) {
    return translator.requestTarget(request, CONTEXT);
  }

  @ParameterizedTest
  @CsvSource({
    "GET, /table/data/file.parquet, false",
    "HEAD, /table/data/file.parquet, false",
    "DELETE, /table/data/file.parquet, false",
    "PUT, /table/data/file.parquet, true",
    "GET, /table/data/file.parquet?response-content-type=text/plain, false",
    "POST, /table/data/file.parquet?uploads, true",
    "PUT, /table/data/file.parquet?partNumber=1&uploadId=abc, true",
    "POST, /table/data/file.parquet?uploadId=abc, true",
    "DELETE, /table/data/file.parquet?uploadId=abc, false",
    "GET, /table/data/file.parquet?x-id=GetObject, false",
  })
  void testObjectRequest(String method, String pathAndQuery, boolean createsObjects) {
    RequestTarget target = target(request(method, BUCKET_URL + pathAndQuery));
    assertThat(target.locations())
        .containsExactly(StorageLocation.of("s3://bucket/table/data/file.parquet"));
    assertThat(target.createsObjects()).isEqualTo(createsObjects);
  }

  @Test
  void testHeadersSentByIcebergClients() {
    Map<String, List<String>> headers =
        Map.of(
            "Host", List.of("bucket.s3.us-west-2.amazonaws.com"),
            "Content-Type", List.of("application/octet-stream"),
            "Content-Length", List.of("123"),
            "amz-sdk-invocation-id", List.of("id"),
            "amz-sdk-request", List.of("attempt=1; max=4"),
            "x-amz-content-sha256", List.of("UNSIGNED-PAYLOAD"),
            "x-amz-sdk-checksum-algorithm", List.of("CRC32"),
            "x-amz-checksum-crc32", List.of("abc"),
            "x-amz-server-side-encryption", List.of("aws:kms"),
            "x-amz-meta-origin", List.of("iceberg"));
    RequestTarget target =
        target(request("PUT", BUCKET_URL + "/table/data/file.parquet", headers, null));
    assertThat(target.createsObjects()).isTrue();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "x-amz-copy-source",
        "X-Amz-Copy-Source",
        "x-amz-acl",
        "x-amz-grant-read",
        "x-amz-object-lock-mode",
        "x-amz-bypass-governance-retention",
        "x-amz-tagging",
        "x-amz-website-redirect-location",
        "x-amz-meta-minio-extract",
        "x-minio-force-delete",
        "cf-anything",
      })
  void testUnsignableHeader(String header) {
    RemoteSignRequest request =
        request("PUT", BUCKET_URL + "/table/file", Map.of(header, List.of("value")), null);
    assertThatThrownBy(() -> target(request))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("cannot be signed");
  }

  @ParameterizedTest
  @ValueSource(strings = {"x_amz_copy_source", "x-amz-côpy-source"})
  void testInvalidHeaderName(String header) {
    RemoteSignRequest request =
        request("PUT", BUCKET_URL + "/table/file", Map.of(header, List.of("value")), null);
    assertThatThrownBy(() -> target(request))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid header name");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "GET /table/file?acl",
        "PUT /table/file?acl",
        "PUT /table/file?tagging",
        "DELETE /table/file?versionId=1",
        "GET /table/file?versionId=1",
        "PUT /table/file?retention",
        "POST /table/file?restore",
        "POST /table/file?select&select-type=2",
        "POST /table/file",
        "PATCH /table/file",
        "OPTIONS /table/file",
        // ListObjects (version 1), ListObjectVersions and multipart uploads in progress
        "GET /?prefix=table/",
        "GET /?versions&prefix=table/",
        "GET /?uploads&prefix=table/",
        // ListObjectsV2 with another parameter, or without a prefix within a directory
        "GET /?list-type=2&prefix=table/&versions",
        "GET /?list-type=2&prefix=table",
      })
  void testUnsignableRequest(String request) {
    String[] parts = request.split(" ");
    assertThatThrownBy(() -> target(request(parts[0], BUCKET_URL + parts[1])))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("cannot be signed");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "/table/file?uploadId=a&uploadId=b",
        "/table/file?partNumber=1;uploadId=a",
        "/table/file?&uploadId=a",
      })
  void testInvalidQuery(String pathAndQuery) {
    assertThatThrownBy(() -> target(request("PUT", BUCKET_URL + pathAndQuery)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @ParameterizedTest
  @ValueSource(strings = {"/table/../other/file", "/table/./file", "/table/..", "/../file"})
  void testRelativeKeySegments(String path) {
    assertThatThrownBy(() -> target(request("GET", BUCKET_URL + path)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid object key");
    String body = deleteObjectsBody("<Object><Key>" + path.substring(1) + "</Key></Object>");
    assertThatThrownBy(() -> target(request("POST", BUCKET_URL + "/?delete", Map.of(), body)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid object key");
  }

  @Test
  void testBodyOnlyForDeleteObjects() {
    RemoteSignRequest request = request("PUT", BUCKET_URL + "/table/file", Map.of(), "body");
    assertThatThrownBy(() -> target(request))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Only DeleteObjects requests can have a body");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        BUCKET_URL + "/?delete",
        BUCKET_URL + "?delete",
        BUCKET_URL + "/?delete&x-id=DeleteObjects",
        "https://s3.us-west-2.amazonaws.com/bucket?delete",
      })
  void testDeleteObjects(String url) {
    boolean pathStyle = url.startsWith("https://s3.");
    String body =
        deleteObjectsBody(
            "<Object><Key>table/data/a.parquet</Key></Object>",
            "<Object>\n  <Key>table/metadata/b&amp;c.avro</Key>\n</Object>");
    RequestTarget target =
        translator.requestTarget(
            request("POST", url, Map.of("Content-MD5", List.of("abc")), body),
            new S3Context(pathStyle, null));
    assertThat(target.locations())
        .containsExactlyInAnyOrder(
            StorageLocation.of("s3://bucket/table/data/a.parquet"),
            StorageLocation.of("s3://bucket/table/metadata/b&c.avro"));
    assertThat(target.createsObjects()).isFalse();
  }

  @Test
  void testDeleteObjectsOfVersions() {
    String body =
        deleteObjectsBody("<Object><Key>table/a.parquet</Key><VersionId>1</VersionId></Object>");
    assertThatThrownBy(() -> target(request("POST", BUCKET_URL + "/?delete", Map.of(), body)))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("Requests that delete a version of an object cannot be signed");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        // No objects
        "<Delete><Quiet>true</Quiet></Delete>",
        // Unexpected elements
        "<Delete><Object><Key>a</Key><Other/></Object></Delete>",
        "<Other><Object><Key>a</Key></Object></Other>",
        // External entities and document type definitions
        "<?xml version=\"1.0\"?><!DOCTYPE Delete [<!ENTITY e SYSTEM \"file:///etc/passwd\">]>"
            + "<Delete><Object><Key>&e;</Key></Object></Delete>",
        "<?xml version=\"1.0\"?><!DOCTYPE Delete [<!ENTITY e \"table/a\">]>"
            + "<Delete><Object><Key>&e;</Key></Object></Delete>",
        // Malformed
        "<Delete><Object><Key>a</Key></Object>",
      })
  void testInvalidDeleteObjectsBody(String body) {
    assertThatThrownBy(() -> target(request("POST", BUCKET_URL + "/?delete", Map.of(), body)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void testTooManyDeleteObjectsKeys() {
    String body = deleteObjectsBody("<Object><Key>table/a</Key></Object>".repeat(1001));
    assertThatThrownBy(() -> target(request("POST", BUCKET_URL + "/?delete", Map.of(), body)))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("between 1 and 1000 objects");
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        // Not a POST request, or without a body
        "DELETE " + BUCKET_URL + "/?delete",
        // Not addressed to the bucket
        "POST " + BUCKET_URL + "/table/?delete",
      })
  void testInvalidDeleteObjectsRequest(String request) {
    String[] parts = request.split(" ");
    String body =
        parts[0].equals("POST") ? deleteObjectsBody("<Object><Key>a</Key></Object>") : null;
    assertThatThrownBy(() -> target(request(parts[0], parts[1], Map.of(), body)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void testDeleteObjectsWithOtherParameters() {
    String body = deleteObjectsBody("<Object><Key>a</Key></Object>");
    assertThatThrownBy(
            () -> target(request("POST", BUCKET_URL + "/?delete&uploads", Map.of(), body)))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("Requests with query parameter 'uploads' cannot be signed");
  }

  @ParameterizedTest
  @CsvSource({
    BUCKET_URL + "/?list-type=2&prefix=table/, s3://bucket/table/",
    BUCKET_URL + "?list-type=2&prefix=table/data/, s3://bucket/table/data/",
    BUCKET_URL + "/?list-type=2&prefix=table/data/00, s3://bucket/table/data/",
    BUCKET_URL
        + "/?list-type=2&prefix=table%2Fdata%2F&continuation-token=abc&delimiter=%2F"
        + "&encoding-type=url&fetch-owner=false&max-keys=1000&start-after=table%2Fdata%2Fa"
        + "&x-id=ListObjectsV2, s3://bucket/table/data/",
    "https://s3.us-west-2.amazonaws.com/bucket?list-type=2&prefix=table/, s3://bucket/table/",
  })
  void testListObjects(String url, String location) {
    boolean pathStyle = url.startsWith("https://s3.");
    RequestTarget target =
        translator.requestTarget(request("GET", url), new S3Context(pathStyle, null));
    assertThat(target.locations()).containsExactly(StorageLocation.of(location));
    assertThat(target.createsObjects()).isFalse();
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        // Not a ListObjectsV2 request
        "GET " + BUCKET_URL + "/?list-type=1&prefix=table/",
        "HEAD " + BUCKET_URL + "/?list-type=2&prefix=table/",
        // Without a prefix
        "GET " + BUCKET_URL + "/?list-type=2",
        "GET " + BUCKET_URL + "/?list-type=2&prefix=",
        "GET " + BUCKET_URL + "/?list-type=2&prefix=table/&prefix=other/",
        // Prefixes that may describe other keys than those listed
        "GET " + BUCKET_URL + "/?list-type=2&prefix=table/../other/",
        "GET " + BUCKET_URL + "/?list-type=2&prefix=table/./data/",
        "GET " + BUCKET_URL + "/?list-type=2&prefix=table//data/",
        "GET " + BUCKET_URL + "/?list-type=2&prefix=/table/",
        "GET " + BUCKET_URL + "/?list-type=2&prefix=table+name/",
        "GET " + BUCKET_URL + "/?list-type=2&prefix=table%FF/",
        "GET " + BUCKET_URL + "/?list-type=2&prefix=table%0A/",
        // Not addressed to the bucket only
        "GET " + BUCKET_URL + "/table/?list-type=2&prefix=table/",
        "GET https://s3.us-west-2.amazonaws.com/buck%65t?list-type=2&prefix=table/",
      })
  void testInvalidListObjectsRequest(String request) {
    String[] parts = request.split(" ");
    boolean pathStyle = parts[1].startsWith("https://s3.");
    assertThatThrownBy(() ->
            translator.requestTarget(request(parts[0], parts[1]), new S3Context(pathStyle, null)))
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  void testListObjectsWithBody() {
    RemoteSignRequest request =
        request("GET", BUCKET_URL + "/?list-type=2&prefix=table/", Map.of(), "body");
    assertThatThrownBy(() -> target(request))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid ListObjectsV2 request");
  }
}
