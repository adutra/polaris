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
package org.apache.polaris.service.catalog.spi;

import java.util.Set;
import org.apache.iceberg.rest.Endpoint;
import org.apache.polaris.core.entity.CatalogEntity;

/**
 * Contributes endpoints advertised in catalog configuration responses.
 *
 * <p>Implementations are discovered through CDI as {@code CatalogConfigEndpointContributor} beans.
 */
@FunctionalInterface
public interface CatalogConfigEndpointContributor {

  /** Returns the endpoints to advertise, regardless of the catalog. */
  Set<Endpoint> endpoints();

  /**
   * Returns the endpoints to advertise for the given catalog.
   *
   * <p>Implementations whose endpoints depend on catalog-level configuration should override this
   * method. The default implementation delegates to {@link #endpoints()}.
   *
   * @param catalog the catalog for which the configuration is being requested
   */
  default Set<Endpoint> endpoints(CatalogEntity catalog) {
    return endpoints();
  }
}
