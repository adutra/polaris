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

import static org.apache.polaris.service.catalog.AccessDelegationMode.REMOTE_SIGNING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.net.URI;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.rest.requests.CreateTableRequest;
import org.apache.iceberg.rest.requests.ImmutableRemoteSignRequest;
import org.apache.iceberg.rest.requests.RemoteSignRequest;
import org.apache.iceberg.rest.responses.LoadTableResponse;
import org.apache.iceberg.rest.responses.RemoteSignResponse;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.auth.PolarisPrincipalAttributes;
import org.apache.polaris.core.collection.ImmutableAttributeMap;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.core.storage.RemoteSigningProperty;
import org.apache.polaris.core.storage.StorageLocation;
import org.apache.polaris.service.Profiles;
import org.apache.polaris.service.admin.PolarisAuthzTestBase;
import org.apache.polaris.service.admin.PolarisAuthzTestsFactory;
import org.apache.polaris.service.storage.sign.RemoteSigningContext;
import org.apache.polaris.service.storage.sign.RemoteSigningTokenService;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for the remote signing endpoint.
 *
 * <p>Tests for create table, load table and register table with remote signing delegation are in
 * {@link AbstractIcebergCatalogHandlerAuthzTest}.
 */
@QuarkusTest
@TestProfile(RemoteSigningAuthzTest.Profile.class)
public class RemoteSigningAuthzTest extends PolarisAuthzTestBase {

  public static class Profile extends Profiles.PolarisAuthzBaseProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .putAll(super.getConfigOverrides())
          // Static credentials, so that requests can be signed without any AWS access
          .put("polaris.storage.aws.access-key", "test-access-key")
          .put("polaris.storage.aws.secret-key", "test-secret-key")
          .build();
    }
  }

  @Inject IcebergCatalogHandlerFactory icebergCatalogHandlerFactory;
  @Inject RemoteSigningTokenService remoteSigningTokenService;

  @Override
  protected PolarisAuthzTestsFactory.Builder authzTestsBuilder(String operationName) {
    return super.authzTestsBuilder(operationName).useFreshRequestContext(true);
  }

  private IcebergCatalogHandler newHandler() {
    PolarisPrincipal authenticatedPrincipal =
        PolarisPrincipal.of(
            principalEntity.getName(),
            ImmutableAttributeMap.builder()
                .put(PolarisPrincipalAttributes.PRINCIPAL_ENTITY_ATTRIBUTE_KEY, principalEntity)
                .put(PolarisPrincipalAttributes.PRINCIPAL_ROLE_ALL_ATTRIBUTE_KEY, true)
                .build(),
            Set.of());
    return icebergCatalogHandlerFactory.createHandler(CATALOG_NAME, authenticatedPrincipal);
  }

  /** Returns a token for the given table context, issued to the test principal. */
  private String token(
      String realmId, TableIdentifier table, String location, boolean stagedCreate) {
    return token(realmId, table, location, stagedCreate, principalEntity.getName());
  }

  private String token(
      String realmId,
      TableIdentifier table,
      String location,
      boolean stagedCreate,
      String principalName) {
    return remoteSigningTokenService.issueToken(
        new RemoteSigningContext(
            realmId,
            CATALOG_NAME,
            table,
            "load-id",
            Set.of(StorageLocation.of(location).normalize()),
            stagedCreate),
        principalName);
  }

  /** Returns a sign request for an object under the given location, with the given token. */
  private static RemoteSignRequest signRequest(String method, String location, String token) {
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

  /** Returns a sign request for an object of the given table, with a token for that table. */
  private RemoteSignRequest signRequest(String method, String realmId, TableIdentifier table) {
    String location = baseCatalog.loadTable(table).location();
    return signRequest(method, location, token(realmId, table, location, false));
  }

  private void grantManageContent() {
    assertSuccess(
        newRootAdminService()
            .grantPrivilegeOnCatalogToRole(
                CATALOG_NAME, CATALOG_ROLE1, PolarisPrivilege.CATALOG_MANAGE_CONTENT));
  }

  @TestFactory
  Stream<DynamicNode> testSignReadRequestPrivileges() {
    String realmId = realmContextHolder.get().getRealmIdentifier();
    return authzTestsBuilder("signReadRequest")
        .action(
            () -> newHandler().signRequest(signRequest("GET", realmId, TABLE_NS1A_2), TABLE_NS1A_2))
        .shouldPassWith(PolarisPrivilege.TABLE_READ_DATA, PolarisPrivilege.TABLE_REMOTE_SIGN)
        .shouldPassWith(PolarisPrivilege.TABLE_WRITE_DATA, PolarisPrivilege.TABLE_REMOTE_SIGN)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldFailWith(PolarisPrivilege.TABLE_READ_DATA)
        .shouldFailWith(PolarisPrivilege.TABLE_REMOTE_SIGN)
        .shouldFailWith(PolarisPrivilege.TABLE_FULL_METADATA, PolarisPrivilege.TABLE_READ_DATA)
        .shouldFailWith(PolarisPrivilege.CATALOG_MANAGE_METADATA, PolarisPrivilege.TABLE_READ_DATA)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testSignWriteRequestPrivileges() {
    String realmId = realmContextHolder.get().getRealmIdentifier();
    return authzTestsBuilder("signWriteRequest")
        .action(
            () -> newHandler().signRequest(signRequest("PUT", realmId, TABLE_NS1A_2), TABLE_NS1A_2))
        .shouldPassWith(PolarisPrivilege.TABLE_WRITE_DATA, PolarisPrivilege.TABLE_REMOTE_SIGN)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldFailWith(PolarisPrivilege.TABLE_READ_DATA, PolarisPrivilege.TABLE_REMOTE_SIGN)
        .shouldFailWith(PolarisPrivilege.TABLE_WRITE_DATA)
        .createTests();
  }

  @TestFactory
  Stream<DynamicNode> testSignStagedCreateRequestPrivileges() {
    String realmId = realmContextHolder.get().getRealmIdentifier();
    TableIdentifier staged = TableIdentifier.of(NS2, "staged");
    String location = "s3://my-bucket/authz/ns2/staged";
    return authzTestsBuilder("signStagedCreateRequest")
        .action(() ->
            newHandler()
                .signRequest(
                    signRequest("PUT", location, token(realmId, staged, location, true)), staged))
        .shouldPassWith(
            PolarisPrivilege.TABLE_CREATE,
            PolarisPrivilege.TABLE_WRITE_DATA,
            PolarisPrivilege.TABLE_REMOTE_SIGN)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldFailWith(PolarisPrivilege.TABLE_CREATE, PolarisPrivilege.TABLE_WRITE_DATA)
        .shouldFailWith(PolarisPrivilege.TABLE_WRITE_DATA, PolarisPrivilege.TABLE_REMOTE_SIGN)
        .createTests();
  }

  @Test
  void testSignRequestOutsideTableLocations() {
    grantManageContent();
    String realmId = realmContextHolder.get().getRealmIdentifier();

    // An object of another table, with a token for the table of the request
    String otherLocation = baseCatalog.loadTable(TABLE_NS1_1).location();
    String tableLocation = baseCatalog.loadTable(TABLE_NS1A_2).location();
    RemoteSignRequest request =
        signRequest("GET", otherLocation, token(realmId, TABLE_NS1A_2, tableLocation, false));

    assertThatThrownBy(() -> newHandler().signRequest(request, TABLE_NS1A_2))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("is not in the list of allowed locations");
  }

  /** Returns a sign request for an object under the given location, without a token. */
  private static RemoteSignRequest signRequestWithoutToken(String method, String location) {
    return ImmutableRemoteSignRequest.builder()
        .from(signRequest(method, location, "unused"))
        .properties(Map.of())
        .build();
  }

  /** Clients older than Iceberg 1.12 do not send the token. */
  @TestFactory
  Stream<DynamicNode> testSignWriteRequestWithoutTokenPrivileges() {
    return authzTestsBuilder("signWriteRequestWithoutToken")
        .action(() ->
            newHandler()
                .signRequest(
                    signRequestWithoutToken("PUT", baseCatalog.loadTable(TABLE_NS1A_2).location()),
                    TABLE_NS1A_2))
        .shouldPassWith(PolarisPrivilege.TABLE_WRITE_DATA, PolarisPrivilege.TABLE_REMOTE_SIGN)
        .shouldPassWith(PolarisPrivilege.CATALOG_MANAGE_CONTENT)
        .shouldFailWith(PolarisPrivilege.TABLE_READ_DATA, PolarisPrivilege.TABLE_REMOTE_SIGN)
        .shouldFailWith(PolarisPrivilege.TABLE_WRITE_DATA)
        .createTests();
  }

  @Test
  void testSignRequestWithoutTokenOutsideTableLocations() {
    grantManageContent();
    String otherLocation = baseCatalog.loadTable(TABLE_NS1_1).location();
    RemoteSignRequest request = signRequestWithoutToken("GET", otherLocation);

    assertThatThrownBy(() -> newHandler().signRequest(request, TABLE_NS1A_2))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("is not in the list of allowed locations");
  }

  /** Without a token, a sign request cannot be for a table being created in a staged create. */
  @Test
  void testSignRequestWithoutTokenForMissingTable() {
    grantManageContent();
    TableIdentifier staged = TableIdentifier.of(NS2, "staged");
    RemoteSignRequest request = signRequestWithoutToken("PUT", "s3://my-bucket/authz/ns2/staged");

    assertThatThrownBy(() -> newHandler().signRequest(request, staged))
        .isInstanceOf(NoSuchTableException.class);
  }

  @ParameterizedTest
  @ValueSource(strings = {"GET", "HEAD", "PUT", "POST", "DELETE"})
  void testSignRequest(String method) {
    grantManageContent();
    String realmId = realmContextHolder.get().getRealmIdentifier();

    RemoteSignResponse response =
        newHandler().signRequest(signRequest(method, realmId, TABLE_NS1A_2), TABLE_NS1A_2);

    assertThat(response.headers()).containsKey("Authorization");
  }

  @Test
  void testSignRequestWithInvalidToken() {
    grantManageContent();
    String location = baseCatalog.loadTable(TABLE_NS1A_2).location();
    RemoteSignRequest request = signRequest("GET", location, "invalid-token");

    assertThatThrownBy(() -> newHandler().signRequest(request, TABLE_NS1A_2))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("Invalid or expired remote signing token");
  }

  @Test
  void testSignRequestWithInvalidUri() {
    grantManageContent();
    String realmId = realmContextHolder.get().getRealmIdentifier();
    RemoteSignRequest request =
        ImmutableRemoteSignRequest.builder()
            .from(signRequest("GET", realmId, TABLE_NS1A_2))
            .uri(URI.create("https://invalid.com"))
            .build();

    assertThatThrownBy(() -> newHandler().signRequest(request, TABLE_NS1A_2))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid S3 URL: https://invalid.com");
  }

  @Test
  void testSignRequestWithInvalidProvider() {
    grantManageContent();
    String realmId = realmContextHolder.get().getRealmIdentifier();
    RemoteSignRequest request =
        ImmutableRemoteSignRequest.builder()
            .from(signRequest("GET", realmId, TABLE_NS1A_2))
            .provider("INVALID")
            .build();

    assertThatThrownBy(() -> newHandler().signRequest(request, TABLE_NS1A_2))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("Invalid remote signing storage provider: INVALID");
  }

  @ParameterizedTest
  @CsvSource({"GCS, GCS", "gcs, GCS", "ADLS, AZURE", "adls, AZURE"})
  void testSignRequestWithUnsupportedProvider(String provider, String storageType) {
    grantManageContent();
    String realmId = realmContextHolder.get().getRealmIdentifier();
    RemoteSignRequest request =
        ImmutableRemoteSignRequest.builder()
            .from(signRequest("GET", realmId, TABLE_NS1A_2))
            .provider(provider)
            .build();

    assertThatThrownBy(() -> newHandler().signRequest(request, TABLE_NS1A_2))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("Remote signing is not supported for storage type: " + storageType);
  }

  @Test
  void testSignRequestWithTokenForAnotherTable() {
    grantManageContent();
    String realmId = realmContextHolder.get().getRealmIdentifier();

    RemoteSignRequest request = signRequest("GET", realmId, TABLE_NS1_1);

    assertThatThrownBy(() -> newHandler().signRequest(request, TABLE_NS1A_2))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("Remote signing token is for a different table");
  }

  @Test
  void testSignRequestWithTokenForAnotherPrincipal() {
    grantManageContent();
    String realmId = realmContextHolder.get().getRealmIdentifier();
    String location = baseCatalog.loadTable(TABLE_NS1A_2).location();
    RemoteSignRequest request =
        signRequest(
            "GET", location, token(realmId, TABLE_NS1A_2, location, false, "another-principal"));

    assertThatThrownBy(() -> newHandler().signRequest(request, TABLE_NS1A_2))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("Invalid or expired remote signing token");
  }

  @Test
  void testStagedCreate() {
    grantManageContent();
    CreateTableRequest createRequest =
        CreateTableRequest.builder().withName("staged").withSchema(SCHEMA).stageCreate().build();

    LoadTableResponse response =
        newHandler()
            .createTableStaged(NS2, createRequest, EnumSet.of(REMOTE_SIGNING), Optional.empty());

    String token =
        response.remoteSigningConfig().properties().get(RemoteSigningProperty.TOKEN.propertyName());
    TableIdentifier staged = TableIdentifier.of(NS2, "staged");
    assertThat(
            remoteSigningTokenService.verifyToken(token, principalEntity.getName()).stagedCreate())
        .isTrue();

    // The staged table does not exist yet, but its objects can be written
    RemoteSignRequest signRequest = signRequest("PUT", response.tableMetadata().location(), token);
    assertThat(newHandler().signRequest(signRequest, staged).headers())
        .containsKey("Authorization");
  }
}
