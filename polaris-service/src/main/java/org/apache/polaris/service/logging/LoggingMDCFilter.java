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
package org.apache.polaris.service.logging;

import io.quarkus.vertx.web.RouteFilter;
import io.vertx.ext.web.RoutingContext;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.UUID;
import org.apache.polaris.core.context.RealmContext;
import org.slf4j.MDC;

@ApplicationScoped
public class LoggingMDCFilter {

  private static final String REQUEST_ID_MDC_KEY = "requestId";
  private static final String REALM_ID_MDC_KEY = "realmId";

  @Inject RealmContext realmContext;

  @Inject LoggingConfiguration loggingConfiguration;

  @RouteFilter
  public void applyMDCContext(RoutingContext rc) {
    loggingConfiguration.mdc().forEach(MDC::put);
    var requestId = rc.request().getHeader(loggingConfiguration.requestIdHeaderName());
    MDC.put(REQUEST_ID_MDC_KEY, requestId == null ? UUID.randomUUID().toString() : requestId);
    MDC.put(REALM_ID_MDC_KEY, realmContext.getRealmIdentifier());
    rc.addEndHandler(
        (v) -> {
          MDC.remove(REQUEST_ID_MDC_KEY);
          MDC.remove(REALM_ID_MDC_KEY);
          loggingConfiguration.mdc().keySet().forEach(MDC::remove);
        });
    rc.next();
  }
}
