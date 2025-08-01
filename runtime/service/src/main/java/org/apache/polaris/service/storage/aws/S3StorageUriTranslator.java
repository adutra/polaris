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
import jakarta.annotation.Nullable;
import jakarta.enterprise.context.ApplicationScoped;
import java.net.URI;
import java.util.Locale;
import org.apache.polaris.service.storage.StorageUriTranslator;

@ApplicationScoped
@Identifier("S3")
public class S3StorageUriTranslator implements StorageUriTranslator {

  private static final String AWS_SUFFIX = ".amazonaws.com";
  private static final String CHINA_SUFFIX = ".amazonaws.com.cn";

  /**
   * Translates HTTP(S) S3 URLs into s3:// URIs.
   *
   * <p>This implementation handles both virtual-hosted style and path style S3 URLs, and handles
   * both AWS S3 and S3-compatible ones.
   *
   * <p>It also handles the so-called <a
   * href="https://iceberg.apache.org/docs/nightly/aws/#object-store-file-layout">object store file
   * layout</a>.
   *
   * <p>The implementation is optimized for performance and does not use regular expressions.
   */
  @Override
  public String translate(URI uri, @Nullable URI endpointUri) {

    String scheme = uri.getScheme();
    if (!"https".equalsIgnoreCase(scheme) && !"http".equalsIgnoreCase(scheme)) {
      throw new IllegalArgumentException("Invalid S3 URL: " + uri);
    }

    String host = uri.getHost();
    if (host == null) {
      throw new IllegalArgumentException("Invalid S3 URL: " + uri);
    }
    host = host.toLowerCase(Locale.ROOT);

    String prefix;
    boolean pathStyle;
    if (endpointUri != null) {
      String endpointHost = endpointUri.getHost();
      if (host.endsWith(endpointHost)) {
        prefix =
            host.length() == endpointHost.length()
                ? ""
                : host.substring(0, host.length() - endpointHost.length() - 1);
      } else {
        throw new IllegalArgumentException("Invalid S3 URL: " + uri);
      }
      pathStyle = prefix.isEmpty();
    } else {
      if (host.endsWith(CHINA_SUFFIX)) {
        prefix = host.substring(0, host.length() - CHINA_SUFFIX.length());
      } else if (host.endsWith(AWS_SUFFIX)) {
        prefix = host.substring(0, host.length() - AWS_SUFFIX.length());
      } else {
        throw new IllegalArgumentException("Invalid S3 URL: " + uri);
      }
      pathStyle = prefix.startsWith("s3.") || prefix.startsWith("s3-") || prefix.equals("s3");
    }

    String path = uri.getPath();
    path = path == null ? "" : path;

    String bucket;
    String key;

    if (pathStyle) {
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
    } else if (endpointUri != null) {
      // Virtual hosted style with endpoint: the bucket is the prefix
      bucket = prefix;
      key = path;
    } else {
      // The separator between the bucket and the service is always the last dot followed by "s3"
      int splitIndex = prefix.lastIndexOf(".s3");
      if (splitIndex > 0) {
        bucket = prefix.substring(0, splitIndex);
        key = path;
      } else {
        throw new IllegalArgumentException("Invalid S3 URL: " + uri);
      }
    }

    return "s3://" + bucket + key;
  }
}
