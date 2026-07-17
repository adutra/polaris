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

package org.apache.polaris.service.authz;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.common.collect.ImmutableMap;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.QuarkusTestProfile;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import java.util.Map;
import org.junit.jupiter.api.Test;

@QuarkusTest
@TestProfile(AuthorizationConfigurationTest.Profile.class)
public class AuthorizationConfigurationTest {

  @Inject AuthorizationConfiguration authzConfig;

  @Test
  void smokeTest() {
    assertThat(authzConfig.type()).isEqualTo("opa");

    Map<String, AuthorizationRealmConfiguration> realms = authzConfig.realms();
    assertThat(realms).hasSize(2);
    assertThat(realms.get("realm1").type()).isEqualTo("internal");
    assertThat(realms.get("realm2").type()).isEqualTo("ranger");
  }

  @Test
  void forRealmFallsBackToGlobalDefault() {
    assertThat(authzConfig.forRealm("unknown-realm").type()).isEqualTo("opa");
  }

  @Test
  void forRealmReturnsRealmSpecific() {
    assertThat(authzConfig.forRealm("realm1").type()).isEqualTo("internal");
    assertThat(authzConfig.forRealm("realm2").type()).isEqualTo("ranger");
  }

  public static class Profile implements QuarkusTestProfile {
    @Override
    public Map<String, String> getConfigOverrides() {
      return ImmutableMap.<String, String>builder()
          // Global default: OPA authorizer
          .put("polaris.authorization.type", "opa")
          // Per-realm overrides
          .put("polaris.authorization.realms.realm1.type", "internal")
          .put("polaris.authorization.realms.realm2.type", "ranger")
          .build();
    }
  }
}
