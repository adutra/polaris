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
package org.apache.polaris.service.storage.s3.sign;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import java.net.URI;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.polaris.core.auth.PolarisPrincipal;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.service.admin.PolarisAuthzTestBase;
import org.apache.polaris.service.s3.sign.model.ImmutablePolarisS3SignRequest;
import org.apache.polaris.service.s3.sign.model.ImmutablePolarisS3SignResponse;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

@QuarkusTest
@TestProfile(PolarisAuthzTestBase.Profile.class)
@SuppressWarnings("resource")
public class S3RemoteSigningCatalogHandlerAuthzTest extends PolarisAuthzTestBase {

  private static final ImmutablePolarisS3SignRequest READ_REQUEST =
      ImmutablePolarisS3SignRequest.builder()
          .method("GET")
          .uri(URI.create("https://example-bucket.s3.amazonaws.com/some-object"))
          .region("us-west-2")
          .build();

  private static final ImmutablePolarisS3SignRequest WRITE_REQUEST =
      ImmutablePolarisS3SignRequest.builder()
          .method("PUT")
          .uri(URI.create("https://example-bucket.s3.amazonaws.com/some-object"))
          .region("us-west-2")
          .build();

  @Test
  public void testReadRequestInsufficientPermissions() {
    doTestInsufficientPrivileges(
        List.of(PolarisPrivilege.TABLE_REMOTE_SIGN, PolarisPrivilege.TABLE_READ_DATA),
        () -> newHandler(newS3SignerMock()).signS3Request(READ_REQUEST, TABLE_NS1_1));
  }

  @Test
  public void testWriteRequestInsufficientPermissions() {
    doTestInsufficientPrivileges(
        List.of(PolarisPrivilege.TABLE_REMOTE_SIGN, PolarisPrivilege.TABLE_WRITE_DATA),
        () -> newHandler(newS3SignerMock()).signS3Request(WRITE_REQUEST, TABLE_NS1_1));
  }

  @Test
  public void testReadRequestSufficientPermissions() {
    doTestSufficientPrivilegeSets(
        List.of(Set.of(PolarisPrivilege.TABLE_READ_DATA, PolarisPrivilege.TABLE_REMOTE_SIGN)),
        () -> newHandler(newS3SignerMock()).signS3Request(READ_REQUEST, TABLE_NS1_1),
        () -> {},
        PRINCIPAL_NAME,
        (privilege) ->
            adminService.grantPrivilegeOnCatalogToRole(CATALOG_NAME, CATALOG_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnCatalogFromRole(CATALOG_NAME, CATALOG_ROLE1, privilege));
  }

  @Test
  public void testWriteRequestSufficientPermissions() {
    doTestSufficientPrivilegeSets(
        List.of(Set.of(PolarisPrivilege.TABLE_WRITE_DATA, PolarisPrivilege.TABLE_REMOTE_SIGN)),
        () -> newHandler(newS3SignerMock()).signS3Request(WRITE_REQUEST, TABLE_NS1_1),
        () -> {},
        PRINCIPAL_NAME,
        (privilege) ->
            adminService.grantPrivilegeOnCatalogToRole(CATALOG_NAME, CATALOG_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnCatalogFromRole(CATALOG_NAME, CATALOG_ROLE1, privilege));
  }

  @Test
  public void testInvalidUriThrowsBadRequest() {
    // Given - grant sufficient permissions first
    adminService.grantPrivilegeOnCatalogToRole(
        CATALOG_NAME, CATALOG_ROLE1, PolarisPrivilege.TABLE_READ_DATA);
    adminService.grantPrivilegeOnCatalogToRole(
        CATALOG_NAME, CATALOG_ROLE1, PolarisPrivilege.TABLE_REMOTE_SIGN);

    // Create a request with an invalid URI (not an S3 URI)
    URI invalidUri = URI.create("https://example.com/some/path");
    ImmutablePolarisS3SignRequest invalidRequest =
        ImmutablePolarisS3SignRequest.builder()
            .method("GET")
            .uri(invalidUri)
            .region("us-west-2")
            .build();

    // Create a mock signer that throws ValidationException for invalid URI
    S3RequestSigner brokenSigner = Mockito.mock(S3RequestSigner.class);
    Mockito.when(brokenSigner.normalizeLocationUri(invalidUri))
        .thenThrow(new ValidationException("Invalid S3 URL: %s", invalidUri));

    // When/Then - should throw BadRequestException wrapping ValidationException
    assertThatThrownBy(() -> newHandler(brokenSigner).signS3Request(invalidRequest, TABLE_NS1_1))
        .isInstanceOf(BadRequestException.class)
        .hasMessageContaining("Invalid request URI");
  }

  private S3RequestSigner newS3SignerMock() {
    S3RequestSigner s3signer = Mockito.mock(S3RequestSigner.class);
    Mockito.when(s3signer.normalizeLocationUri(any()))
        .thenAnswer(invocation -> "s3://example-bucket/some-object");
    Mockito.when(s3signer.signRequest(any()))
        .thenReturn(ImmutablePolarisS3SignResponse.builder().uri(URI.create("irrelevant")).build());
    return s3signer;
  }

  private S3RemoteSigningCatalogHandler newHandler(S3RequestSigner s3signer) {
    PolarisPrincipal principal = PolarisPrincipal.of(principalEntity, Set.of());
    return new S3RemoteSigningCatalogHandler(
        diagServices,
        callContext,
        resolutionManifestFactory,
        principal,
        CATALOG_NAME,
        polarisAuthorizer,
        s3signer);
  }

  private void doTestInsufficientPrivileges(
      List<PolarisPrivilege> insufficientPrivileges, Runnable action) {
    doTestInsufficientPrivileges(
        insufficientPrivileges,
        PRINCIPAL_NAME,
        action,
        (privilege) ->
            adminService.grantPrivilegeOnCatalogToRole(CATALOG_NAME, CATALOG_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnCatalogFromRole(CATALOG_NAME, CATALOG_ROLE1, privilege));
  }
}
