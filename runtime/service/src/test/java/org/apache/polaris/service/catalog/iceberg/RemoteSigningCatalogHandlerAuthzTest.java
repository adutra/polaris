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

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.enterprise.inject.Any;
import jakarta.enterprise.inject.Instance;
import jakarta.inject.Inject;
import jakarta.ws.rs.core.UriInfo;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.rest.requests.ImmutableRemoteSignRequest;
import org.apache.iceberg.rest.requests.RemoteSignRequest;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.catalog.ExternalCatalogFactory;
import org.apache.polaris.core.storage.RemoteSigningProperty;
import org.apache.polaris.service.admin.PolarisAuthzTestBase;
import org.apache.polaris.service.catalog.CatalogPrefixParser;
import org.apache.polaris.service.context.catalog.CallContextCatalogFactory;
import org.apache.polaris.service.storage.StorageUriTranslator;
import org.apache.polaris.service.storage.sign.RemoteSigner;
import org.apache.polaris.service.storage.sign.RemoteSigningToken;
import org.apache.polaris.service.storage.sign.RemoteSigningTokenService;
import org.junit.jupiter.api.Test;

/**
 * Authz tests for the remote signing endpoint authorization logic .
 *
 * <p>Tests for create and load table with remote signing delegation are in {@link
 * AbstractIcebergCatalogHandlerAuthzTest}.
 *
 * <p>General note: we cannot use {@link #doTestSufficientPrivilegeSets(List, Runnable, Runnable,
 * String, Function, Function)} or {@link #doTestInsufficientPrivilegeSets(List, String, Runnable,
 * Function, Function)} because the authz checks here are done against the pre-encoded privileges
 * stored in the remote signing token.
 */
@QuarkusTest
@TestProfile(PolarisAuthzTestBase.Profile.class)
@SuppressWarnings("resource")
public class RemoteSigningCatalogHandlerAuthzTest extends PolarisAuthzTestBase {

  @Inject CallContextCatalogFactory callContextCatalogFactory;
  @Inject @Any Instance<ExternalCatalogFactory> externalCatalogFactories;
  @Inject CatalogPrefixParser prefixParser;
  @Inject UriInfo uriInfo;
  @Inject @Any Instance<RemoteSigner> requestSigners;
  @Inject @Any Instance<StorageUriTranslator> uriTranslators;
  @Inject RemoteSigningTokenService remoteSigningTokenService;

  @Test
  public void testMissingToken() {
    RemoteSignRequest requestWithInvalidToken =
        ImmutableRemoteSignRequest.builder()
            .uri(URI.create("https://example-bucket.s3.amazonaws.com/some-object"))
            .method("GET")
            .region("us-west-2")
            .build();
    assertThatThrownBy(() -> newHandler().signRequest(requestWithInvalidToken, TABLE_NS1_1))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("Missing remote signing token");
  }

  @Test
  public void testInvalidToken() {
    RemoteSignRequest requestWithInvalidToken =
        ImmutableRemoteSignRequest.builder()
            .uri(URI.create("https://example-bucket.s3.amazonaws.com/some-object"))
            .method("GET")
            .region("us-west-2")
            .properties(Map.of(RemoteSigningProperty.TOKEN.shortName(), "invalid-token"))
            .build();
    assertThatThrownBy(() -> newHandler().signRequest(requestWithInvalidToken, TABLE_NS1_1))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("Invalid or expired remote signing token");
  }

  @Test
  public void testTokenForDifferentCatalog() {
    RemoteSigningToken token =
        RemoteSigningToken.builder()
            .catalogName("different-catalog")
            .tableIdentifier(TABLE_NS1_1)
            .allowedLocations(Set.of("s3://example-bucket/"))
            .readWrite(true)
            .principalName(PRINCIPAL_NAME)
            .build();
    RemoteSignRequest request =
        ImmutableRemoteSignRequest.builder()
            .uri(URI.create("https://example-bucket.s3.amazonaws.com/some-object"))
            .method("GET")
            .region("us-west-2")
            .properties(
                Map.of(
                    RemoteSigningProperty.TOKEN.shortName(),
                    remoteSigningTokenService.encrypt(token),
                    RemoteSigningProperty.STORAGE_TYPE.shortName(),
                    "S3"))
            .build();
    assertThatThrownBy(() -> newHandler().signRequest(request, TABLE_NS1_1))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("Signed token is for a different catalog");
  }

  @Test
  public void testTokenForDifferentTable() {
    RemoteSigningToken token =
        RemoteSigningToken.builder()
            .catalogName(CATALOG_NAME)
            .tableIdentifier(TableIdentifier.of(Namespace.of("other"), "table"))
            .allowedLocations(Set.of("s3://example-bucket/"))
            .readWrite(true)
            .principalName(PRINCIPAL_NAME)
            .build();
    RemoteSignRequest request =
        ImmutableRemoteSignRequest.builder()
            .uri(URI.create("https://example-bucket.s3.amazonaws.com/some-object"))
            .method("GET")
            .region("us-west-2")
            .properties(
                Map.of(
                    RemoteSigningProperty.TOKEN.shortName(),
                    remoteSigningTokenService.encrypt(token),
                    RemoteSigningProperty.STORAGE_TYPE.shortName(),
                    "S3"))
            .build();
    assertThatThrownBy(() -> newHandler().signRequest(request, TABLE_NS1_1))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("Signed token is for a different table");
  }

  @Test
  public void testTokenForDifferentPrincipal() {
    RemoteSigningToken token =
        RemoteSigningToken.builder()
            .catalogName(CATALOG_NAME)
            .tableIdentifier(TABLE_NS1_1)
            .allowedLocations(Set.of("s3://example-bucket/"))
            .readWrite(true)
            .principalName("different-principal")
            .build();
    RemoteSignRequest request =
        ImmutableRemoteSignRequest.builder()
            .uri(URI.create("https://example-bucket.s3.amazonaws.com/some-object"))
            .method("GET")
            .region("us-west-2")
            .properties(
                Map.of(
                    RemoteSigningProperty.TOKEN.shortName(),
                    remoteSigningTokenService.encrypt(token),
                    RemoteSigningProperty.STORAGE_TYPE.shortName(),
                    "S3"))
            .build();
    assertThatThrownBy(() -> newHandler().signRequest(request, TABLE_NS1_1))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("Signed token is for a different principal");
  }

  @Test
  public void testWriteOperationNotAllowed() {
    // Create a token that only allows read operations
    RemoteSigningToken token =
        RemoteSigningToken.builder()
            .catalogName(CATALOG_NAME)
            .tableIdentifier(TABLE_NS1_1)
            .allowedLocations(Set.of("s3://example-bucket/"))
            .readWrite(false)
            .principalName(PRINCIPAL_NAME)
            .build();
    // But try to make a write request (PUT)
    RemoteSignRequest writeRequest =
        ImmutableRemoteSignRequest.builder()
            .uri(URI.create("https://example-bucket.s3.amazonaws.com/some-object"))
            .method("PUT")
            .region("us-west-2")
            .properties(
                Map.of(
                    RemoteSigningProperty.TOKEN.shortName(),
                    remoteSigningTokenService.encrypt(token),
                    RemoteSigningProperty.STORAGE_TYPE.shortName(),
                    "S3"))
            .build();
    assertThatThrownBy(() -> newHandler().signRequest(writeRequest, TABLE_NS1_1))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("Not authorized to sign write request");
  }

  @Test
  public void testInvalidRequestUri() {
    URI invalidUri = URI.create("https://example.com/some/path");
    RemoteSigningToken token =
        RemoteSigningToken.builder()
            .catalogName(CATALOG_NAME)
            .tableIdentifier(TABLE_NS1_1)
            .allowedLocations(Set.of("s3://example-bucket/"))
            .readWrite(false)
            .readWrite(true)
            .principalName(PRINCIPAL_NAME)
            .build();
    RemoteSignRequest invalidRequest =
        ImmutableRemoteSignRequest.builder()
            .uri(invalidUri)
            .method("GET")
            .region("us-west-2")
            .properties(
                Map.of(
                    RemoteSigningProperty.TOKEN.shortName(),
                    remoteSigningTokenService.encrypt(token),
                    RemoteSigningProperty.STORAGE_TYPE.shortName(),
                    "S3"))
            .build();
    assertThatThrownBy(() -> newHandler().signRequest(invalidRequest, TABLE_NS1_1))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("Invalid request URI");
  }

  @Test
  public void testMissingStorageType() {
    RemoteSigningToken token =
        RemoteSigningToken.builder()
            .catalogName(CATALOG_NAME)
            .tableIdentifier(TABLE_NS1_1)
            .allowedLocations(Set.of("s3://example-bucket/"))
            .readWrite(false)
            .readWrite(true)
            .principalName(PRINCIPAL_NAME)
            .build();
    RemoteSignRequest request =
        ImmutableRemoteSignRequest.builder()
            .uri(URI.create("https://example-bucket.s3.amazonaws.com/some-object"))
            .method("GET")
            .region("us-west-2")
            .properties(
                Map.of(
                    RemoteSigningProperty.TOKEN.shortName(),
                    remoteSigningTokenService.encrypt(token)))
            .build();
    assertThatThrownBy(() -> newHandler().signRequest(request, TABLE_NS1_1))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("Missing remote signing storage type");
  }

  @Test
  public void testInvalidStorageType() {
    RemoteSigningToken token =
        RemoteSigningToken.builder()
            .catalogName(CATALOG_NAME)
            .tableIdentifier(TABLE_NS1_1)
            .allowedLocations(Set.of("s3://example-bucket/"))
            .readWrite(false)
            .readWrite(true)
            .principalName(PRINCIPAL_NAME)
            .build();
    RemoteSignRequest request =
        ImmutableRemoteSignRequest.builder()
            .uri(URI.create("https://example-bucket.s3.amazonaws.com/some-object"))
            .method("GET")
            .region("us-west-2")
            .properties(
                Map.of(
                    RemoteSigningProperty.TOKEN.shortName(),
                    remoteSigningTokenService.encrypt(token),
                    RemoteSigningProperty.STORAGE_TYPE.shortName(),
                    "INVALID"))
            .build();
    assertThatThrownBy(() -> newHandler().signRequest(request, TABLE_NS1_1))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("Invalid remote signing storage type");
  }

  @Test
  public void testUnsupportedStorageType() {
    RemoteSigningToken token =
        RemoteSigningToken.builder()
            .catalogName(CATALOG_NAME)
            .tableIdentifier(TABLE_NS1_1)
            .allowedLocations(Set.of("s3://example-bucket/"))
            .readWrite(false)
            .readWrite(true)
            .principalName(PRINCIPAL_NAME)
            .build();
    RemoteSignRequest request =
        ImmutableRemoteSignRequest.builder()
            .uri(URI.create("https://example-bucket.s3.amazonaws.com/some-object"))
            .method("GET")
            .region("us-west-2")
            .properties(
                Map.of(
                    RemoteSigningProperty.TOKEN.shortName(),
                    remoteSigningTokenService.encrypt(token),
                    RemoteSigningProperty.STORAGE_TYPE.shortName(),
                    "GCS"))
            .build();
    assertThatThrownBy(() -> newHandler().signRequest(request, TABLE_NS1_1))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("Remote signing is not supported for storage type: GCS");
  }

  private IcebergCatalogHandler newHandler() {
    return new IcebergCatalogHandler(
        diagServices,
        callContext,
        prefixParser,
        resolverFactory,
        resolutionManifestFactory,
        metaStoreManager,
        credentialManager,
        PolarisPrincipal.of(principalEntity, Set.of()),
        callContextCatalogFactory,
        CATALOG_NAME,
        polarisAuthorizer,
        reservedProperties,
        catalogHandlerUtils,
        externalCatalogFactories,
        storageAccessConfigProvider,
        eventAttributeMap,
        uriInfo,
        requestSigners,
        uriTranslators,
        remoteSigningTokenService);
  }
}
