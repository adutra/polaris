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
package org.apache.polaris.extension.persistence.impl.eclipselink;

import io.quarkus.arc.lookup.LookupIfProperty;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.polaris.core.PolarisDiagnostics;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.core.persistence.LocalPolarisMetaStoreManagerFactory;
import org.apache.polaris.core.persistence.PolarisMetaStoreManager;
import org.apache.polaris.core.persistence.PolarisMetaStoreSession;
import org.apache.polaris.service.config.RuntimeCandidate;
import org.eclipse.microprofile.config.inject.ConfigProperty;

/**
 * The implementation of Configuration interface for configuring the {@link PolarisMetaStoreManager}
 * using an EclipseLink based meta store to store and retrieve all Polaris metadata. It can be
 * configured through persistence.xml to use supported RDBMS as the meta store.
 */
@ApplicationScoped
@RuntimeCandidate
@LookupIfProperty(name = "polaris.persistence.metastore-manager.type", stringValue = "eclipselink")
public class EclipseLinkPolarisMetaStoreManagerFactory
    extends LocalPolarisMetaStoreManagerFactory<PolarisEclipseLinkStore> {

  @ConfigProperty(name = "polaris.eclipselink.conf-file")
  private String confFile;

  @ConfigProperty(name = "polaris.eclipselink.persistence-unit", defaultValue = "polaris")
  private String persistenceUnitName;

  @Override
  protected PolarisEclipseLinkStore createBackingStore(PolarisDiagnostics diagnostics) {
    return new PolarisEclipseLinkStore(diagnostics);
  }

  @Override
  protected PolarisMetaStoreSession createMetaStoreSession(
      PolarisEclipseLinkStore store, RealmContext realmContext) {
    return new PolarisEclipseLinkMetaStoreSessionImpl(
        store, storageIntegration, realmContext, confFile, persistenceUnitName);
  }
}
