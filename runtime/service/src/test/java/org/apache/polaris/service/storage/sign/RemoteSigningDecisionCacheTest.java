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
package org.apache.polaris.service.storage.sign;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.polaris.core.storage.StorageLocation;
import org.junit.jupiter.api.Test;

class RemoteSigningDecisionCacheTest {

  private static final TableIdentifier TABLE = TableIdentifier.of(Namespace.of("ns"), "table");

  private static final RemoteSigningPermissions PERMISSIONS =
      RemoteSigningPermissions.builder()
          .allowedLocations(Set.of(StorageLocation.of("s3://bucket/table")))
          .build();

  private static final RemoteSigningDecisionCache.Key READ_KEY = key("principal", false);

  private final AtomicInteger authorizations = new AtomicInteger();

  private RemoteSigningPermissions authorize() {
    authorizations.incrementAndGet();
    return PERMISSIONS;
  }

  @Test
  void testDisabledCacheAuthorizesEveryRequest() {
    RemoteSigningDecisionCache cache = new RemoteSigningDecisionCache(configuration(Duration.ZERO));

    assertThat(cache.getOrAuthorize(READ_KEY, this::authorize)).isEqualTo(PERMISSIONS);
    assertThat(cache.getOrAuthorize(READ_KEY, this::authorize)).isEqualTo(PERMISSIONS);

    assertThat(authorizations).hasValue(2);
  }

  @Test
  void testEnabledCacheReusesDecisions() {
    RemoteSigningDecisionCache cache =
        new RemoteSigningDecisionCache(configuration(Duration.ofMinutes(1)));

    assertThat(cache.getOrAuthorize(READ_KEY, this::authorize)).isEqualTo(PERMISSIONS);
    assertThat(cache.getOrAuthorize(READ_KEY, this::authorize)).isEqualTo(PERMISSIONS);
    assertThat(authorizations).hasValue(1);

    // Different access modes and principals are authorized separately
    cache.getOrAuthorize(key("principal", true), this::authorize);
    cache.getOrAuthorize(key("other-principal", false), this::authorize);
    assertThat(authorizations).hasValue(3);
  }

  @Test
  void testFailedAuthorizationsAreNotCached() {
    RemoteSigningDecisionCache cache =
        new RemoteSigningDecisionCache(configuration(Duration.ofMinutes(1)));

    Supplier<RemoteSigningPermissions> denying =
        () -> {
          authorizations.incrementAndGet();
          throw new ForbiddenException("denied");
        };
    assertThatThrownBy(() -> cache.getOrAuthorize(READ_KEY, denying))
        .isInstanceOf(ForbiddenException.class);

    assertThat(cache.getOrAuthorize(READ_KEY, this::authorize)).isEqualTo(PERMISSIONS);
    assertThat(authorizations).hasValue(2);
  }

  private static RemoteSigningDecisionCache.Key key(String principal, boolean write) {
    return new RemoteSigningDecisionCache.Key(
        "realm", principal, Set.of("role"), "catalog", TABLE, write);
  }

  private static RemoteSigningConfiguration configuration(Duration ttl) {
    RemoteSigningConfiguration configuration = mock(RemoteSigningConfiguration.class);
    when(configuration.decisionCacheTtl()).thenReturn(ttl);
    return configuration;
  }
}
