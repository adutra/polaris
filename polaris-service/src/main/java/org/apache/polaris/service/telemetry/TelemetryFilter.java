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
package org.apache.polaris.service.telemetry;

import io.opentelemetry.api.trace.Span;
import io.quarkus.vertx.web.RouteFilter;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.apache.polaris.service.logging.LoggingMDCFilter;
import org.eclipse.microprofile.config.inject.ConfigProperty;

@ApplicationScoped
public class TelemetryFilter {

  @ConfigProperty(name = "quarkus.otel.sdk.disabled")
  boolean sdkDisabled;

  @Inject TelemetryConfiguration telemetryConfiguration;

  @RouteFilter(LoggingMDCFilter.PRIORITY - 1)
  public void applySpanAttributes(RoutingContext rc) {
    if (!sdkDisabled) {
      Span span = Span.current();
      String requestId = rc.get(LoggingMDCFilter.REQUEST_ID_KEY);
      if (requestId != null) {
        span.setAttribute(LoggingMDCFilter.REQUEST_ID_KEY, requestId);
      }
      String realmId = rc.get(LoggingMDCFilter.REALM_ID_KEY);
      span.setAttribute(LoggingMDCFilter.REALM_ID_KEY, realmId);
      telemetryConfiguration.spanAttributes().forEach(span::setAttribute);
    }
    rc.next();
  }
}
