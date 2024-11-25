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
package org.apache.polaris.service.context;

import io.vertx.core.http.HttpServerRequest;
import jakarta.ws.rs.container.ContainerRequestContext;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.polaris.core.context.RealmContext;

public interface RealmContextResolver {

  RealmContext resolveRealmContext(
      String requestURL,
      String method,
      String path,
      Map<String, String> queryParams,
      Map<String, String> headers);

  default RealmContext resolveRealmContext(HttpServerRequest request) {
    return resolveRealmContext(
        request.absoluteURI(),
        request.method().name(),
        request.path(),
        request.params().entries().stream()
            .collect(HashMap::new, (m, e) -> m.put(e.getKey(), e.getValue()), HashMap::putAll),
        request.headers().entries().stream()
            .collect(HashMap::new, (m, e) -> m.put(e.getKey(), e.getValue()), HashMap::putAll));
  }

  default RealmContext resolveRealmContext(ContainerRequestContext requestContext) {
    String path = requestContext.getUriInfo().getPath();
    Map<String, String> queryParams =
        requestContext.getUriInfo().getQueryParameters().entrySet().stream()
            .collect(Collectors.toMap(Entry::getKey, (e) -> e.getValue().getFirst()));
    Map<String, String> headers =
        requestContext.getHeaders().entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, (e) -> e.getValue().getFirst()));
    return resolveRealmContext(
        requestContext.getUriInfo().getRequestUri().toString(),
        requestContext.getMethod(),
        path,
        queryParams,
        headers);
  }

  String getDefaultRealm();
}
