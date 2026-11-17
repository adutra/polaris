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
package org.apache.polaris.service.catalog.iceberg;

import jakarta.annotation.Priority;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.Set;
import org.apache.iceberg.rest.Endpoint;
import org.apache.polaris.core.config.FeatureConfiguration;
import org.apache.polaris.core.config.RealmConfig;
import org.apache.polaris.core.entity.CatalogEntity;
import org.apache.polaris.service.catalog.spi.CatalogConfigEndpointContributor;

/**
 * Advertises the Iceberg REST remote signing endpoint when remote signing is enabled, either for
 * the whole realm or for a specific catalog.
 */
@ApplicationScoped
@Priority(150)
public class IcebergRemoteSigningConfigEndpoints implements CatalogConfigEndpointContributor {
  private static final Set<Endpoint> REMOTE_SIGNING_ENDPOINTS =
      Set.of(Endpoint.V1_TABLE_REMOTE_SIGN);

  private final RealmConfig realmConfig;

  @Inject
  public IcebergRemoteSigningConfigEndpoints(RealmConfig realmConfig) {
    this.realmConfig = realmConfig;
  }

  @Override
  public Set<Endpoint> endpoints() {
    return realmConfig.getConfig(FeatureConfiguration.REMOTE_SIGNING_ENABLED)
        ? REMOTE_SIGNING_ENDPOINTS
        : Set.of();
  }

  @Override
  public Set<Endpoint> endpoints(CatalogEntity catalog) {
    // Must be kept in sync with the checks in IcebergCatalogHandler
    return !catalog.isExternal()
            && realmConfig.getConfig(FeatureConfiguration.REMOTE_SIGNING_ENABLED, catalog)
        ? REMOTE_SIGNING_ENDPOINTS
        : Set.of();
  }
}
