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

import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import java.net.URI;
import java.util.List;
import java.util.Set;
import org.apache.iceberg.rest.requests.ImmutableRemoteSignRequest;
import org.apache.iceberg.rest.requests.RemoteSignRequest;
import org.apache.polaris.core.entity.PolarisPrivilege;
import org.apache.polaris.service.admin.PolarisAuthzTestBase;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(PolarisAuthzTestBase.Profile.class)
@SuppressWarnings("resource")
public class S3RemoteSigningCatalogHandlerAuthzTest extends AbstractIcebergCatalogHandlerAuthzTest {

  private static final RemoteSignRequest READ_REQUEST =
      ImmutableRemoteSignRequest.builder()
          .method("GET")
          .uri(URI.create("https://example-bucket.s3.amazonaws.com/some-object"))
          .region("us-west-2")
          .build();

  private static final RemoteSignRequest WRITE_REQUEST =
      ImmutableRemoteSignRequest.builder()
          .method("PUT")
          .uri(URI.create("https://example-bucket.s3.amazonaws.com/some-object"))
          .region("us-west-2")
          .build();

  @Test
  public void testReadRequestInsufficientPermissions() {
    doTestInsufficientPrivileges(
        List.of(PolarisPrivilege.TABLE_REMOTE_SIGN, PolarisPrivilege.TABLE_READ_DATA),
        () -> newWrapper().signS3Request(READ_REQUEST, TABLE_NS1_1));
  }

  @Test
  public void testWriteRequestInsufficientPermissions() {
    doTestInsufficientPrivileges(
        List.of(PolarisPrivilege.TABLE_REMOTE_SIGN, PolarisPrivilege.TABLE_WRITE_DATA),
        () -> newWrapper().signS3Request(WRITE_REQUEST, TABLE_NS1_1));
  }

  @Test
  public void testReadRequestSufficientPermissions() {
    doTestSufficientPrivilegeSets(
        List.of(Set.of(PolarisPrivilege.TABLE_READ_DATA, PolarisPrivilege.TABLE_REMOTE_SIGN)),
        () -> newWrapper().signS3Request(READ_REQUEST, TABLE_NS1_1),
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
        () -> newWrapper().signS3Request(WRITE_REQUEST, TABLE_NS1_1),
        () -> {},
        PRINCIPAL_NAME,
        (privilege) ->
            adminService.grantPrivilegeOnCatalogToRole(CATALOG_NAME, CATALOG_ROLE1, privilege),
        (privilege) ->
            adminService.revokePrivilegeOnCatalogFromRole(CATALOG_NAME, CATALOG_ROLE1, privilege));
  }
}
