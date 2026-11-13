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

import io.smallrye.common.annotation.Identifier;
import jakarta.enterprise.context.ApplicationScoped;
import java.net.URI;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import org.apache.iceberg.rest.requests.RemoteSignRequest;
import org.apache.polaris.core.storage.StorageLocation;
import org.apache.polaris.core.storage.aws.S3Location;
import org.apache.polaris.service.storage.StorageLocationTranslator;
import org.jspecify.annotations.Nullable;

@ApplicationScoped
@Identifier("S3")
public class S3StorageLocationTranslator implements StorageLocationTranslator {

  /**
   * @param pathStyleAccess whether URIs use path-style access
   * @param endpoint the custom S3 endpoint that URIs target, or null for AWS S3 endpoints
   */
  public record S3Context(boolean pathStyleAccess, @Nullable URI endpoint) implements Context {}

  /**
   * Translates HTTP(S) S3 URLs into s3:// URIs.
   *
   * <p>This implementation handles both virtual-hosted style and path style S3 URLs, and handles
   * both AWS S3 and S3-compatible ones.
   *
   * <p>The host of the URL must match the addressing style and the endpoint of the context, so that
   * the URL is interpreted in the same way as by the object store. When the context has an
   * endpoint, path-style URLs must target the endpoint's host and port, and virtual-hosted-style
   * URLs a subdomain of it, named after the bucket. Otherwise, the URL must target an AWS S3
   * endpoint, with no bucket in the host for path-style URLs.
   *
   * <p>The implementation is optimized for performance and does not use regular expressions.
   *
   * <p>The implementation is able to handle bucket names with dots in path style access. When using
   * virtual-hosted style, it is able to handle bucket names with dots when the endpoint is a custom
   * one, or when the bucket name is followed by ".s3", e.g. {@code
   * my.bucket.s3.us-east-1.amazonaws.com}.
   */
  @Override
  public StorageLocation translate(URI uri, Context context) {

    String scheme = uri.getScheme();
    if (!"https".equalsIgnoreCase(scheme) && !"http".equalsIgnoreCase(scheme)) {
      throw new IllegalArgumentException("Invalid S3 URL: " + uri);
    }

    String host = uri.getHost();
    if (host == null) {
      throw new IllegalArgumentException("Invalid S3 URL: " + uri);
    }
    host = host.toLowerCase(Locale.ROOT);

    String path = uri.getPath();
    path = path == null ? "" : path;

    S3Context s3Context = (S3Context) context;
    URI endpoint = s3Context.endpoint();

    String bucket;
    String key;

    if (s3Context.pathStyleAccess()) {
      boolean validHost =
          endpoint != null
              ? isEndpointHost(uri, host, endpoint)
              : isAwsHost(host)
                  && (host.startsWith("s3.") || host.startsWith("s3-"))
                  // A ".s3" label after the first one denotes a bucket, e.g. s3-bucket.s3.<region>
                  && !host.contains(".s3");
      if (!validHost) {
        throw new IllegalArgumentException("Invalid S3 URL: " + uri);
      }
      if (path.isEmpty() || path.equals("/")) {
        throw new IllegalArgumentException("Invalid S3 URL: " + uri); // No bucket in path
      }
      int slashIndex = path.indexOf('/', 1);
      if (slashIndex == -1) {
        // Path is just the bucket: /mybucket
        bucket = path.substring(1);
        key = "";
      } else {
        // Path is bucket + key: /mybucket/myimage.jpg
        bucket = path.substring(1, slashIndex);
        key = path.substring(slashIndex);
      }
    } else {
      int bucketEndIndex;
      if (endpoint != null) {
        // The bucket is the subdomain of the endpoint's host
        String endpointHost = endpointHost(endpoint);
        bucketEndIndex =
            endpointHost != null
                    && host.endsWith("." + endpointHost)
                    && effectivePort(uri) == effectivePort(endpoint)
                ? host.length() - endpointHost.length() - 1
                : -1;
      } else {
        // All official S3 endpoints have ".s3" as the bucket separator
        bucketEndIndex = isAwsHost(host) ? host.lastIndexOf(".s3") : -1;
      }
      if (bucketEndIndex <= 0) {
        throw new IllegalArgumentException("Invalid S3 URL: " + uri);
      }
      bucket = host.substring(0, bucketEndIndex);
      key = path;
    }

    if (bucket.isEmpty()) {
      throw new IllegalArgumentException("Invalid S3 URL: " + uri);
    }

    return new S3Location("s3://" + bucket + key);
  }

  /**
   * Validates a request to sign against {@link S3SignRequestPolicy}, and returns what it accesses.
   * A {@code DeleteObjects} request addresses its bucket, and accesses the objects whose keys are
   * in its body.
   */
  @Override
  public RequestTarget requestTarget(RemoteSignRequest request, Context context) {
    S3SignRequestPolicy.RequestKind kind = S3SignRequestPolicy.validate(request);
    StorageLocation location = translate(request.uri(), context);
    String method = request.method().toUpperCase(Locale.ROOT);
    if (kind == S3SignRequestPolicy.RequestKind.OBJECT) {
      S3SignRequestPolicy.validateKey(location.toString());
      return new RequestTarget(Set.of(location), method.equals("PUT") || method.equals("POST"));
    }
    // The location of the bucket: s3://bucket, or s3://bucket/
    String bucket = location.toString().substring("s3://".length());
    bucket = bucket.endsWith("/") ? bucket.substring(0, bucket.length() - 1) : bucket;
    if (bucket.contains("/")) {
      throw new IllegalArgumentException("Invalid DeleteObjects request URL: " + request.uri());
    }
    Set<StorageLocation> locations = new HashSet<>();
    for (String key : S3SignRequestPolicy.deleteObjectsKeys(request.body())) {
      S3SignRequestPolicy.validateKey(key);
      locations.add(new S3Location("s3://" + bucket + "/" + key));
    }
    return new RequestTarget(locations, false);
  }

  private static boolean isAwsHost(String host) {
    return host.endsWith(".amazonaws.com") || host.endsWith(".amazonaws.com.cn");
  }

  private static boolean isEndpointHost(URI uri, String host, URI endpoint) {
    return host.equals(endpointHost(endpoint)) && effectivePort(uri) == effectivePort(endpoint);
  }

  private static @Nullable String endpointHost(URI endpoint) {
    String host = endpoint.getHost();
    return host == null ? null : host.toLowerCase(Locale.ROOT);
  }

  private static int effectivePort(URI uri) {
    if (uri.getPort() != -1) {
      return uri.getPort();
    }
    return "https".equalsIgnoreCase(uri.getScheme()) ? 443 : 80;
  }
}
