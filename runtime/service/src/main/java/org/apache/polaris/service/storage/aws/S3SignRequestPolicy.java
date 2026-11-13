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

import java.io.StringReader;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.rest.requests.RemoteSignRequest;

/**
 * The requests to S3 that Polaris signs: those that Iceberg clients send to read, write and delete
 * the files of a table.
 *
 * <p>S3 and compatible object stores dispatch on query parameters and on {@code x-} headers, so
 * that a parameter or header that is not listed here could make a signed request access more than
 * the locations of the table. Both are refused unless listed.
 */
final class S3SignRequestPolicy {

  /** What a request to sign addresses. */
  enum RequestKind {
    /** One object, whose key is the path of the request. */
    OBJECT,
    /** {@code DeleteObjects}: the objects whose keys are listed in the body of the request. */
    DELETE_OBJECTS
  }

  /** The query parameter identifying a {@code DeleteObjects} request. */
  static final String DELETE_QUERY_PARAMETER = "delete";

  /**
   * The query parameters of object requests: multipart uploads, and a telemetry parameter that S3
   * ignores. None of them reaches another object than the one in the path.
   */
  private static final Set<String> OBJECT_QUERY_PARAMETERS =
      Set.of("partNumber", "uploadId", "uploads", "x-id");

  /**
   * Query parameters overriding the headers of a {@code GetObject} or {@code HeadObject} response.
   */
  private static final String RESPONSE_QUERY_PARAMETER_PREFIX = "response-";

  private static final Set<String> DELETE_OBJECTS_QUERY_PARAMETERS =
      Set.of(DELETE_QUERY_PARAMETER, "x-id");

  private static final Set<String> METHODS = Set.of("GET", "HEAD", "PUT", "POST", "DELETE");

  /**
   * The {@code x-} headers that can be signed: those set by the signer, integrity checks and
   * transfer encodings, conditions that only narrow the request, and properties of the written
   * object.
   */
  private static final Set<String> SIGNABLE_X_HEADERS =
      Set.of(
          "x-amz-content-sha256",
          "x-amz-date",
          "x-amz-security-token",
          "x-amzn-trace-id",
          "x-amz-decoded-content-length",
          "x-amz-mp-object-size",
          "x-amz-sdk-checksum-algorithm",
          "x-amz-te",
          "x-amz-trailer",
          "x-amz-expected-bucket-owner",
          "x-amz-request-payer",
          "x-amz-storage-class",
          "x-amz-server-side-encryption",
          "x-amz-server-side-encryption-aws-kms-key-id",
          "x-amz-server-side-encryption-context",
          "x-amz-server-side-encryption-bucket-key-enabled",
          "x-amz-server-side-encryption-customer-algorithm",
          "x-amz-server-side-encryption-customer-key",
          "x-amz-server-side-encryption-customer-key-md5");

  /** Checksums of any algorithm, and user metadata. */
  private static final List<String> SIGNABLE_X_HEADER_PREFIXES =
      List.of("x-amz-checksum-", "x-amz-meta-");

  /**
   * Headers that some object stores act on although they are not {@code x-amz-} headers or user
   * metadata, e.g. to store the entries of an archive under their own keys.
   */
  private static final List<String> UNSIGNABLE_HEADER_PREFIXES =
      List.of("x-amz-meta-minio-", "x-amz-meta-snowball-", "cf-", "ibm-");

  /** The maximum number of objects a {@code DeleteObjects} request deletes. */
  private static final int MAX_DELETE_OBJECTS_KEYS = 1000;

  private static final XMLInputFactory XML_INPUT_FACTORY = newXmlInputFactory();

  private S3SignRequestPolicy() {}

  /**
   * Validates the method, the query parameters, the headers and the body of a request to sign, and
   * returns what it addresses.
   *
   * @throws IllegalArgumentException if the request is malformed
   * @throws ForbiddenException if the request is well-formed, but cannot be signed
   */
  static RequestKind validate(RemoteSignRequest request) {
    String method = request.method().toUpperCase(Locale.ROOT);
    if (!METHODS.contains(method)) {
      throw new ForbiddenException("Requests with method %s cannot be signed", request.method());
    }
    Set<String> parameters = queryParameterNames(request.uri().getRawQuery());
    RequestKind kind =
        parameters.contains(DELETE_QUERY_PARAMETER)
            ? RequestKind.DELETE_OBJECTS
            : RequestKind.OBJECT;
    for (String parameter : parameters) {
      boolean allowed =
          kind == RequestKind.DELETE_OBJECTS
              ? DELETE_OBJECTS_QUERY_PARAMETERS.contains(parameter)
              : OBJECT_QUERY_PARAMETERS.contains(parameter)
                  || parameter.startsWith(RESPONSE_QUERY_PARAMETER_PREFIX);
      if (!allowed) {
        throw new ForbiddenException(
            "Requests with query parameter '%s' cannot be signed", parameter);
      }
    }
    if (kind == RequestKind.DELETE_OBJECTS) {
      if (!method.equals("POST") || request.body() == null) {
        throw new IllegalArgumentException("Invalid DeleteObjects request");
      }
    } else {
      if (request.body() != null) {
        throw new IllegalArgumentException("Only DeleteObjects requests can have a body");
      }
      // Multipart uploads are created and completed with POST requests
      if (method.equals("POST")
          && !parameters.contains("uploads")
          && !parameters.contains("uploadId")) {
        throw new ForbiddenException("POST requests without a multipart upload cannot be signed");
      }
    }
    request.headers().keySet().forEach(S3SignRequestPolicy::validateHeader);
    return kind;
  }

  private static void validateHeader(String name) {
    // Names that are not ASCII tokens could be read as another name, and some stores read "_" as
    // "-"
    if (!name.chars().allMatch(c -> c > 0x20 && c < 0x7f) || name.contains("_")) {
      throw new IllegalArgumentException("Invalid header name: " + name);
    }
    String lowerCaseName = name.toLowerCase(Locale.ROOT);
    boolean signable =
        UNSIGNABLE_HEADER_PREFIXES.stream().noneMatch(lowerCaseName::startsWith)
            && (!lowerCaseName.startsWith("x-")
                || SIGNABLE_X_HEADERS.contains(lowerCaseName)
                || SIGNABLE_X_HEADER_PREFIXES.stream().anyMatch(lowerCaseName::startsWith));
    if (!signable) {
      throw new ForbiddenException("Requests with header '%s' cannot be signed", name);
    }
  }

  /**
   * Returns the names of the query parameters of a request. Some servers also split parameters on
   * ";", and which value of a repeated parameter applies is implementation-defined, so both are
   * refused.
   */
  private static Set<String> queryParameterNames(String rawQuery) {
    Set<String> names = new HashSet<>();
    if (rawQuery == null || rawQuery.isEmpty()) {
      return names;
    }
    if (rawQuery.contains(";")) {
      throw new IllegalArgumentException("Invalid query parameter separator: ;");
    }
    for (String parameter : rawQuery.split("&", -1)) {
      int equalsIndex = parameter.indexOf('=');
      String name =
          URLDecoder.decode(
              equalsIndex == -1 ? parameter : parameter.substring(0, equalsIndex),
              StandardCharsets.UTF_8);
      if (name.isEmpty()) {
        throw new IllegalArgumentException("Invalid query parameter: " + parameter);
      }
      if (!names.add(name)) {
        throw new IllegalArgumentException("Repeated query parameter: " + name);
      }
    }
    return names;
  }

  /**
   * Refuses object keys with "." or ".." segments. S3 reads them literally, but other object stores
   * or proxies may resolve them to another key than the one authorized, and table files never have
   * them.
   */
  static void validateKey(String key) {
    for (String segment : key.split("/", -1)) {
      if (segment.equals(".") || segment.equals("..")) {
        throw new IllegalArgumentException("Invalid object key: " + key);
      }
    }
  }

  /**
   * Returns the keys of the objects that the body of a {@code DeleteObjects} request deletes.
   * Deleting a version of an object removes it for good, which no table operation needs, so it is
   * refused.
   */
  static List<String> deleteObjectsKeys(String body) {
    List<String> keys = new ArrayList<>();
    try {
      XMLStreamReader reader = XML_INPUT_FACTORY.createXMLStreamReader(new StringReader(body));
      try {
        List<String> path = new ArrayList<>();
        StringBuilder text = new StringBuilder();
        while (reader.hasNext()) {
          switch (reader.next()) {
            case XMLStreamConstants.DTD, XMLStreamConstants.ENTITY_REFERENCE ->
                throw new IllegalArgumentException("Invalid DeleteObjects request body");
            case XMLStreamConstants.START_ELEMENT -> {
              path.add(reader.getLocalName());
              text.setLength(0);
              validateDeleteObjectsElement(path);
            }
            case XMLStreamConstants.CHARACTERS, XMLStreamConstants.CDATA ->
                text.append(reader.getText());
            case XMLStreamConstants.END_ELEMENT -> {
              if (path.equals(List.of("Delete", "Object", "Key"))) {
                keys.add(text.toString());
              }
              path.removeLast();
            }
            default -> {}
          }
        }
      } finally {
        reader.close();
      }
    } catch (XMLStreamException e) {
      throw new IllegalArgumentException("Invalid DeleteObjects request body", e);
    }
    if (keys.isEmpty() || keys.size() > MAX_DELETE_OBJECTS_KEYS) {
      throw new IllegalArgumentException(
          "DeleteObjects requests must delete between 1 and "
              + MAX_DELETE_OBJECTS_KEYS
              + " objects");
    }
    return keys;
  }

  private static void validateDeleteObjectsElement(List<String> path) {
    boolean valid =
        switch (path.size()) {
          case 1 -> path.getFirst().equals("Delete");
          case 2 -> path.get(1).equals("Object") || path.get(1).equals("Quiet");
          case 3 -> path.get(1).equals("Object") && path.get(2).equals("Key");
          default -> false;
        };
    if (path.size() == 3 && path.get(1).equals("Object") && path.get(2).equals("VersionId")) {
      throw new ForbiddenException("Requests that delete a version of an object cannot be signed");
    }
    if (!valid) {
      throw new IllegalArgumentException(
          "Invalid DeleteObjects request body: unexpected element " + String.join("/", path));
    }
  }

  private static XMLInputFactory newXmlInputFactory() {
    XMLInputFactory factory = XMLInputFactory.newFactory();
    factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
    factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
    return factory;
  }
}
