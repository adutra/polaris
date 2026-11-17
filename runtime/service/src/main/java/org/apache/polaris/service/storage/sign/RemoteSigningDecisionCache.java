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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.time.Duration;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import org.apache.iceberg.catalog.TableIdentifier;

/**
 * A per-node cache of sign request authorizations, used with per-request authorization when {@link
 * RemoteSigningConfiguration#decisionCacheTtl()} is positive.
 */
@ApplicationScoped
public class RemoteSigningDecisionCache {

  private static final long MAXIMUM_SIZE = 10_000;

  /**
   * The cache key: everything an authorization decision depends on.
   *
   * @param write whether the decision is for a write request
   */
  public record Key(
      String realmId,
      String principalName,
      Set<String> activatedRoles,
      String catalogName,
      TableIdentifier tableIdentifier,
      boolean write) {

    public Key {
      activatedRoles = Set.copyOf(activatedRoles);
    }
  }

  private final BiFunction<Key, Supplier<RemoteSigningPermissions>, RemoteSigningPermissions>
      lookup;

  @Inject
  public RemoteSigningDecisionCache(RemoteSigningConfiguration configuration) {
    Duration ttl = configuration.decisionCacheTtl();
    if (ttl.isPositive()) {
      Cache<Key, RemoteSigningPermissions> cache =
          Caffeine.newBuilder().expireAfterWrite(ttl).maximumSize(MAXIMUM_SIZE).build();
      this.lookup = (key, authorizer) -> cache.get(key, k -> authorizer.get());
    } else {
      this.lookup = (key, authorizer) -> authorizer.get();
    }
  }

  /**
   * Returns the permissions cached for the given key, or computes them with the given authorizer.
   * Failed authorizations are not cached.
   */
  public RemoteSigningPermissions getOrAuthorize(
      Key key, Supplier<RemoteSigningPermissions> authorizer) {
    return lookup.apply(key, authorizer);
  }
}
