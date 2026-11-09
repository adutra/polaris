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

import java.net.URI;
import java.util.Set;
import org.apache.polaris.core.storage.StorageLocation;
import org.apache.polaris.immutables.PolarisImmutable;
import org.immutables.value.Value;
import org.jspecify.annotations.Nullable;

/** What a principal is allowed to sign for a table, as obtained by authorizing a sign request. */
@PolarisImmutable
public interface RemoteSigningPermissions {

  static ImmutableRemoteSigningPermissions.Builder builder() {
    return ImmutableRemoteSigningPermissions.builder();
  }

  /** The set of allowed locations that the user can access. */
  Set<StorageLocation> allowedLocations();

  /** Whether request URIs use path-style access. Disabled by default. */
  @Value.Default
  default boolean pathStyleAccess() {
    return false;
  }

  /** The custom S3 endpoint that request URIs target, or null for AWS S3 endpoints. */
  @Nullable URI endpoint();

  /**
   * The name of the storage whose credentials sign requests, or null to use the default
   * credentials.
   */
  @Nullable String storageName();

  /** Check that the permissions are valid. */
  @Value.Check
  default void check() {
    if (allowedLocations().isEmpty()) {
      throw new IllegalArgumentException("No allowed locations");
    }
  }
}
