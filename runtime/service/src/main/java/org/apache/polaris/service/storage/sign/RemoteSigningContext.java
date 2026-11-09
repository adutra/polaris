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

import java.util.Set;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.storage.StorageLocation;

/**
 * The table context recorded in a remote signing token with per-request authorization. It carries
 * no privileges: each sign request is authorized against the caller's current privileges.
 *
 * @param realmId the realm of the table
 * @param catalogName the catalog of the table
 * @param tableIdentifier the table
 * @param loadId an identifier of the request that issued the token, to correlate sign requests with
 *     it
 * @param locations the table's locations when the token was issued; used when the table entity does
 *     not record them (staged tables, and tables not committed since Polaris 1.3.0)
 * @param stagedCreate whether the token was issued for a staged table creation, in which case sign
 *     requests are authorized against the table's namespace
 */
public record RemoteSigningContext(
    String realmId,
    String catalogName,
    TableIdentifier tableIdentifier,
    String loadId,
    Set<StorageLocation> locations,
    boolean stagedCreate) {

  public RemoteSigningContext {
    locations = Set.copyOf(locations);
  }
}
