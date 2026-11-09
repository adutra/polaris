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
package org.apache.polaris.service.it;

import static org.apache.polaris.test.commons.MinioRustProfile.ACCESS_KEY;
import static org.apache.polaris.test.commons.MinioRustProfile.SECRET_KEY;
import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.ResourceArg;
import io.quarkus.test.junit.QuarkusIntegrationTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import org.apache.iceberg.Table;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.io.PositionOutputStream;
import org.apache.iceberg.io.SeekableInputStream;
import org.apache.iceberg.rest.RESTCatalog;
import org.apache.iceberg.rest.RESTCatalogProperties;
import org.apache.polaris.core.storage.StorageAccessProperty;
import org.apache.polaris.service.it.ext.PolarisIntegrationTestExtension;
import org.apache.polaris.service.it.test.PolarisRemoteSigningS3IntegrationTestBase;
import org.apache.polaris.test.commons.MinioRustProfile;
import org.apache.polaris.test.rustfs.Rustfs;
import org.apache.polaris.test.rustfs.RustfsAccess;
import org.apache.polaris.test.rustfs.RustfsConditionExtension;
import org.apache.polaris.test.rustfs.RustfsTestResource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@QuarkusIntegrationTest
@TestProfile(RemoteSigningS3RustFSIT.Profile.class)
@QuarkusTestResource(
    value = RustfsTestResource.class,
    initArgs = {
      @ResourceArg(name = "accessKey", value = ACCESS_KEY),
      @ResourceArg(name = "secretKey", value = SECRET_KEY)
    })
@ExtendWith(RustfsConditionExtension.class)
@ExtendWith(PolarisIntegrationTestExtension.class)
public class RemoteSigningS3RustFSIT extends PolarisRemoteSigningS3IntegrationTestBase {

  private static final String BUCKET_URI_PREFIX = "/rustfs-test-remote-signing/";

  /** Signer endpoint property read by Iceberg clients up to 1.11. */
  private static final String LEGACY_S3_SIGNER_ENDPOINT = "s3.signer.endpoint";

  public static class Profile implements QuarkusTestProfile {

    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          .putAll(MinioRustProfile.CONFIG_OVERRIDES)
          .putAll(DEFAULT_REMOTE_SIGNING_CONFIG)
          .buildKeepingLast();
    }
  }

  @Rustfs static RustfsAccess rustfsAccess;

  @Override
  protected String storageBase() {
    return rustfsAccess.s3BucketUri(BUCKET_URI_PREFIX).toString();
  }

  @Override
  protected Optional<String> endpoint() {
    return Optional.of(rustfsAccess.s3endpoint());
  }

  @Override
  protected ImmutableMap.Builder<String, String> clientFileIOProperties() {
    return super.clientFileIOProperties()
        // Grant direct access to the RustFS bucket; this FileIO instance does not
        // use access delegation.
        .put(StorageAccessProperty.AWS_KEY_ID.getPropertyName(), ACCESS_KEY)
        .put(StorageAccessProperty.AWS_SECRET_KEY.getPropertyName(), SECRET_KEY);
  }

  /**
   * Tests that clients older than Iceberg 1.12, which use the legacy signer endpoint and do not
   * send the remote signing token, can read and write the objects of existing tables.
   */
  @Test
  @SuppressWarnings("deprecation")
  public void testRemoteSigningWithoutToken() throws IOException {
    @SuppressWarnings("resource")
    RESTCatalog catalog = catalog();
    catalog.createNamespace(NS);
    Table table = catalog.createTable(TABLE, SCHEMA);

    @SuppressWarnings("resource")
    Map<String, String> properties = new HashMap<>(table.io().properties());
    properties.remove(RESTCatalogProperties.REMOTE_SIGNING_CONFIG);
    properties.remove(RESTCatalogProperties.REMOTE_SIGNING_ENDPOINT);
    properties.put(
        RESTCatalogProperties.SIGNER_ENDPOINT, properties.get(LEGACY_S3_SIGNER_ENDPOINT));

    try (S3FileIO io = new S3FileIO()) {
      io.initialize(properties);
      String path = table.location() + "/data/" + UUID.randomUUID() + ".bin";
      byte[] content = "content".getBytes(StandardCharsets.UTF_8);
      try (PositionOutputStream out = io.newOutputFile(path).create()) {
        out.write(content);
      }
      try (SeekableInputStream in = io.newInputFile(path).newStream()) {
        assertThat(in.readAllBytes()).isEqualTo(content);
      }
      io.deleteFile(path);
    }
  }
}
