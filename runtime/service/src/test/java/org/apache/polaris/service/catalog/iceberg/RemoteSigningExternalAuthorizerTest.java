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

import static org.assertj.core.api.Assertions.assertThat;

import io.quarkus.test.junit.QuarkusMock;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.net.URI;
import java.util.Map;
import java.util.Set;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.rest.requests.ImmutableRemoteSignRequest;
import org.apache.iceberg.rest.requests.RemoteSignRequest;
import org.apache.polaris.core.auth.PolarisAuthorizer;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.auth.PolarisPrincipalAttributes;
import org.apache.polaris.core.collection.ImmutableAttributeMap;
import org.apache.polaris.core.storage.RemoteSigningProperty;
import org.apache.polaris.core.storage.StorageLocation;
import org.apache.polaris.service.admin.PolarisAuthzTestBase;
import org.apache.polaris.service.auth.oidc.TestPolarisAuthorizer;
import org.apache.polaris.service.storage.sign.RemoteSigningContext;
import org.apache.polaris.service.storage.sign.RemoteSigningTokenService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Remote signing with an external authorizer, which only resolves the entities it needs. */
@QuarkusTest
@TestProfile(RemoteSigningAuthzTest.Profile.class)
public class RemoteSigningExternalAuthorizerTest extends PolarisAuthzTestBase {

  @Inject IcebergCatalogHandlerFactory icebergCatalogHandlerFactory;
  @Inject RemoteSigningTokenService remoteSigningTokenService;

  @BeforeEach
  void installExternalAuthorizer() {
    // After the setup of the base class, which relies on the built-in authorizer
    QuarkusMock.installMockForType(new TestPolarisAuthorizer(), PolarisAuthorizer.class);
  }

  private IcebergCatalogHandler newHandler() {
    PolarisPrincipal principal =
        PolarisPrincipal.of(
            principalEntity.getName(),
            ImmutableAttributeMap.builder()
                .put(PolarisPrincipalAttributes.PRINCIPAL_ENTITY_ATTRIBUTE_KEY, principalEntity)
                .put(PolarisPrincipalAttributes.PRINCIPAL_ROLE_ALL_ATTRIBUTE_KEY, true)
                .build(),
            Set.of());
    return icebergCatalogHandlerFactory.createHandler(CATALOG_NAME, principal);
  }

  private RemoteSignRequest signRequest(
      String method, TableIdentifier table, String location, boolean stagedCreate) {
    String token =
        remoteSigningTokenService.issueToken(
            new RemoteSigningContext(
                realmContextHolder.get().getRealmIdentifier(),
                CATALOG_NAME,
                table,
                null,
                "load-id",
                Set.of(StorageLocation.of(location).normalize()),
                stagedCreate,
                false),
            principalEntity.getName());
    URI uri = URI.create(location);
    return ImmutableRemoteSignRequest.builder()
        .uri(
            URI.create(
                "https://"
                    + uri.getHost()
                    + ".s3.amazonaws.com"
                    + uri.getPath()
                    + "/data/file.parquet"))
        .method(method)
        .region("us-west-2")
        .provider("S3")
        .properties(Map.of(RemoteSigningProperty.TOKEN.propertyName(), token))
        .build();
  }

  @Test
  void testSignRequest() {
    String location = baseCatalog.loadTable(TABLE_NS1A_2).location();
    RemoteSignRequest request = signRequest("GET", TABLE_NS1A_2, location, false);

    assertThat(newHandler().signRequest(request, TABLE_NS1A_2).headers())
        .containsKey("Authorization");
  }

  @Test
  void testSignStagedCreateRequest() {
    TableIdentifier staged = TableIdentifier.of(NS2, "staged");
    RemoteSignRequest request = signRequest("PUT", staged, "s3://my-bucket/authz/ns2/staged", true);

    assertThat(newHandler().signRequest(request, staged).headers()).containsKey("Authorization");
  }
}
