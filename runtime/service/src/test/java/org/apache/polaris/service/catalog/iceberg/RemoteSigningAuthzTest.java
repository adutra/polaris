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
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.EnumSet;
import java.util.List;
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
import org.apache.polaris.service.storage.sign.RemoteSigningConfiguration;
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
          // A fixed key, shared with the token services the tests create
          .put(
              "polaris.remote-signing.keys.default",
              Base64.getEncoder()
                  .encodeToString(
                      "0123456789abcdef0123456789abcdef".getBytes(StandardCharsets.UTF_8)))
          .build();
    }
  }

  @Inject IcebergCatalogHandlerFactory icebergCatalogHandlerFactory;
  @Inject RemoteSigningTokenService remoteSigningTokenService;
  @Inject RemoteSigningConfiguration remoteSigningConfiguration;

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
            null,
            "load-id",
            Set.of(StorageLocation.of(location).normalize()),
            stagedCreate,
            false),
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
  @ValueSource(strings = {"GET", "HEAD", "PUT", "DELETE"})
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
            remoteSigningTokenService
                .verifyToken(token, principalEntity.getName())
                .context()
                .stagedCreate())
        .isTrue();

    // The staged table does not exist yet, but its objects can be written
    RemoteSignRequest signRequest = signRequest("PUT", response.tableMetadata().location(), token);
    assertThat(newHandler().signRequest(signRequest, staged).headers())
        .containsKey("Authorization");
  }

  /** Returns a DeleteObjects request for objects under the given location, with the given token. */
  private static RemoteSignRequest deleteObjectsRequest(
      String location, String token, String... relativeKeys) {
    URI uri = URI.create(location);
    StringBuilder body = new StringBuilder("<Delete>");
    for (String relativeKey : relativeKeys) {
      body.append("<Object><Key>")
          .append(uri.getPath().substring(1))
          .append('/')
          .append(relativeKey)
          .append("</Key></Object>");
    }
    body.append("</Delete>");
    return ImmutableRemoteSignRequest.builder()
        .uri(URI.create("https://" + uri.getHost() + ".s3.amazonaws.com/?delete"))
        .method("POST")
        .region("us-west-2")
        .provider("S3")
        .body(body.toString())
        .properties(Map.of(RemoteSigningProperty.TOKEN.propertyName(), token))
        .build();
  }

  @Test
  void testDeleteObjects() {
    grantManageContent();
    String realmId = realmContextHolder.get().getRealmIdentifier();
    String location = baseCatalog.loadTable(TABLE_NS1A_2).location();
    String token = token(realmId, TABLE_NS1A_2, location, false);

    RemoteSignRequest request =
        deleteObjectsRequest(location, token, "data/a.parquet", "metadata/b.avro");
    assertThat(newHandler().signRequest(request, TABLE_NS1A_2).headers())
        .containsKey("Authorization");

    // Each object must be in the locations of the table
    String otherLocation = baseCatalog.loadTable(TABLE_NS1_1).location();
    RemoteSignRequest outside = deleteObjectsRequest(otherLocation, token, "data/b.parquet");
    assertThatThrownBy(() -> newHandler().signRequest(outside, TABLE_NS1A_2))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("is not in the list of allowed locations");
  }

  @Test
  void testSignRequestWithUnsignableHeader() {
    grantManageContent();
    String realmId = realmContextHolder.get().getRealmIdentifier();
    String otherObject = baseCatalog.loadTable(TABLE_NS1_1).location() + "/data/file.parquet";
    RemoteSignRequest request =
        ImmutableRemoteSignRequest.builder()
            .from(signRequest("PUT", realmId, TABLE_NS1A_2))
            .headers(Map.of("x-amz-copy-source", List.of(otherObject)))
            .build();

    assertThatThrownBy(() -> newHandler().signRequest(request, TABLE_NS1A_2))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("Requests with header 'x-amz-copy-source' cannot be signed");
  }

  /**
   * Creates a table with remote signing, as the test principal holding all privileges on the
   * catalog, and returns its token. The privileges are revoked afterwards.
   */
  private String createTableWithRemoteSigning(TableIdentifier table) {
    grantManageContent();
    LoadTableResponse response =
        newHandler()
            .createTableDirect(
                table.namespace(),
                CreateTableRequest.builder().withName(table.name()).withSchema(SCHEMA).build(),
                EnumSet.of(REMOTE_SIGNING),
                Optional.empty());
    assertSuccess(
        newRootAdminService()
            .revokePrivilegeOnCatalogFromRole(
                CATALOG_NAME, CATALOG_ROLE1, PolarisPrivilege.CATALOG_MANAGE_CONTENT));
    return response
        .remoteSigningConfig()
        .properties()
        .get(RemoteSigningProperty.TOKEN.propertyName());
  }

  @Test
  void testDroppedTable() {
    TableIdentifier dropped = TableIdentifier.of(NS1A, "dropped");
    String token = createTableWithRemoteSigning(dropped);
    String location = baseCatalog.loadTable(dropped).location();
    assertThat(baseCatalog.dropTable(dropped, false)).isTrue();

    // The objects of the table can still be read and deleted, e.g. to purge it. The grants on the
    // table were dropped with it, and the principal holds no privileges anymore: the token alone
    // authorizes the requests.
    for (String method : List.of("HEAD", "GET", "DELETE")) {
      assertThat(newHandler().signRequest(signRequest(method, location, token), dropped).headers())
          .containsKey("Authorization");
    }
    assertThat(
            newHandler()
                .signRequest(deleteObjectsRequest(location, token, "data/file.parquet"), dropped)
                .headers())
        .containsKey("Authorization");

    // But they cannot be written
    assertThatThrownBy(() -> newHandler().signRequest(signRequest("PUT", location, token), dropped))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("can only be read or deleted");

    // Nor accessed without a token
    RemoteSignRequest withoutToken =
        ImmutableRemoteSignRequest.builder()
            .from(signRequest("GET", location, token))
            .properties(Map.of())
            .build();
    assertThatThrownBy(() -> newHandler().signRequest(withoutToken, dropped))
        .isInstanceOf(NoSuchTableException.class);
  }

  @Test
  void testDroppedTableWithTokenWithoutTableId() {
    String realmId = realmContextHolder.get().getRealmIdentifier();
    TableIdentifier dropped = TableIdentifier.of(NS1A, "dropped");
    String location = baseCatalog.createTable(dropped, SCHEMA).location();
    String token = token(realmId, dropped, location, false);
    baseCatalog.dropTable(dropped, false);

    // Without the table's ID, the token cannot tell whether the table was dropped or renamed
    assertThatThrownBy(() -> newHandler().signRequest(signRequest("GET", location, token), dropped))
        .isInstanceOf(NoSuchTableException.class);
  }

  @Test
  void testRenamedTable() {
    TableIdentifier table = TableIdentifier.of(NS1A, "original");
    String token = createTableWithRemoteSigning(table);
    String location = baseCatalog.loadTable(table).location();
    baseCatalog.renameTable(table, TableIdentifier.of(NS1A, "renamed"));

    assertThatThrownBy(() -> newHandler().signRequest(signRequest("GET", location, token), table))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("was renamed: it must be loaded again under its new name");
  }

  @Test
  void testDroppedTableRecreated() {
    TableIdentifier table = TableIdentifier.of(NS1A, "recreated");
    String token = createTableWithRemoteSigning(table);
    String location = baseCatalog.loadTable(table).location();
    baseCatalog.dropTable(table, false);
    baseCatalog.createTable(table, SCHEMA);

    // Requests for the new table are authorized against it
    RemoteSignRequest request = signRequest("GET", location, token);
    assertThatThrownBy(() -> newHandler().signRequest(request, table))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("is not authorized for op REMOTE_SIGN_READ");
    grantManageContent();
    assertThat(newHandler().signRequest(request, table).headers()).containsKey("Authorization");
  }

  @Test
  void testDroppedTableLocationReused() {
    TableIdentifier dropped = TableIdentifier.of(NS1A, "dropped");
    String token = createTableWithRemoteSigning(dropped);
    String location = baseCatalog.loadTable(dropped).location();
    baseCatalog.dropTable(dropped, false);

    // Another table now uses the location of the dropped table
    baseCatalog
        .buildTable(TableIdentifier.of(NS1A, "other"), SCHEMA)
        .withLocation(location)
        .create();

    assertThatThrownBy(() -> newHandler().signRequest(signRequest("GET", location, token), dropped))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("overlaps another table or namespace");
  }

  /** Returns a token for the same context as the given one, which expired a day ago. */
  private String expired(String token) {
    RemoteSigningContext context =
        remoteSigningTokenService.verifyToken(token, principalEntity.getName()).context();
    Clock past = Clock.fixed(Instant.now().minus(Duration.ofDays(2)), ZoneOffset.UTC);
    return new RemoteSigningTokenService(remoteSigningConfiguration, past)
        .issueToken(context, principalEntity.getName());
  }

  /** Returns a token for the same context as the given one, without the purge decision. */
  private String withoutPurge(String token) {
    RemoteSigningContext context =
        remoteSigningTokenService.verifyToken(token, principalEntity.getName()).context();
    return remoteSigningTokenService.issueToken(
        new RemoteSigningContext(
            context.realmId(),
            context.catalogName(),
            context.tableIdentifier(),
            context.tableId(),
            context.loadId(),
            context.locations(),
            context.stagedCreate(),
            false),
        principalEntity.getName());
  }

  @Test
  void testTokenRecordsPurgeDecision() {
    TableIdentifier table = TableIdentifier.of(NS1A, "purgeable");
    String token = createTableWithRemoteSigning(table);

    RemoteSigningContext context =
        remoteSigningTokenService.verifyToken(token, principalEntity.getName()).context();
    assertThat(context.purgeAllowed()).isTrue();
    assertThat(context.tableId()).isNotNull();
  }

  @Test
  void testExpiredTokenForExistingTable() {
    grantManageContent();
    String realmId = realmContextHolder.get().getRealmIdentifier();
    String location = baseCatalog.loadTable(TABLE_NS1A_2).location();
    String token = expired(token(realmId, TABLE_NS1A_2, location, false));

    // The table exists, so the token is not needed to authorize the request
    assertThat(
            newHandler().signRequest(signRequest("GET", location, token), TABLE_NS1A_2).headers())
        .containsKey("Authorization");
  }

  @Test
  void testDroppedTableWithExpiredToken() {
    TableIdentifier dropped = TableIdentifier.of(NS1A, "dropped");
    String token = expired(createTableWithRemoteSigning(dropped));
    String location = baseCatalog.loadTable(dropped).location();
    baseCatalog.dropTable(dropped, false);

    assertThatThrownBy(() -> newHandler().signRequest(signRequest("GET", location, token), dropped))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("Invalid or expired remote signing token");
  }

  @Test
  void testDroppedTableWithoutPurgeDecision() {
    TableIdentifier dropped = TableIdentifier.of(NS1A, "dropped");
    String token = withoutPurge(createTableWithRemoteSigning(dropped));
    String location = baseCatalog.loadTable(dropped).location();
    baseCatalog.dropTable(dropped, false);

    assertThatThrownBy(() -> newHandler().signRequest(signRequest("GET", location, token), dropped))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("Not authorized to access the objects of dropped table");
  }

  @Test
  void testStagedCreateTokenForExistingTable() {
    String realmId = realmContextHolder.get().getRealmIdentifier();
    TableIdentifier staged = TableIdentifier.of(NS2, "staged");
    String location = "s3://my-bucket/authz/ns2/staged";
    String token = token(realmId, staged, location, true);
    // The table now exists, e.g. created by the staged create transaction, or by another one
    baseCatalog.buildTable(staged, SCHEMA).withLocation(location).create();

    // Requests are authorized against the table, rather than its namespace
    RemoteSignRequest request = signRequest("PUT", location, token);
    assertThatThrownBy(() -> newHandler().signRequest(request, staged))
        .isInstanceOf(ForbiddenException.class)
        .hasMessageContaining("is not authorized for op REMOTE_SIGN_WRITE");
    grantManageContent();
    assertThat(newHandler().signRequest(request, staged).headers()).containsKey("Authorization");
  }
}
