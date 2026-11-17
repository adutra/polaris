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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.map;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.smallrye.config.PropertiesConfigSource;
import io.smallrye.config.SmallRyeConfigBuilder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.Map;
import java.util.Set;
import javax.crypto.SecretKey;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.storage.StorageLocation;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class RemoteSigningTokenServiceTest {

  private static final String TEST_SECRET_KEY_1 =
      Base64.getEncoder()
          .encodeToString(
              "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                  .getBytes(StandardCharsets.UTF_8));

  private static final String TEST_SECRET_KEY_2 =
      Base64.getEncoder()
          .encodeToString(
              "fedcba9876543210fedcba9876543210fedcba9876543210fedcba9876543210"
                  .getBytes(StandardCharsets.UTF_8));

  private static final String TEST_REALM = "test-realm";

  private static final String TEST_KEY_ID_1 = "test-key-1";

  private static final String TEST_KEY_ID_2 = "test-key-2";

  private static final String TEST_PRINCIPAL = "test-principal";

  private static final Duration TOKEN_LIFESPAN = Duration.ofMinutes(15);

  private static final TableIdentifier TEST_TABLE =
      TableIdentifier.of(Namespace.of("ns1"), "table1");

  private static final RemoteSigningContext TEST_CONTEXT =
      new RemoteSigningContext(
          TEST_REALM,
          "test-catalog",
          TEST_TABLE,
          42L,
          "load-1",
          Set.of(StorageLocation.of("s3://bucket/path/")),
          false,
          true);

  private Clock clock;
  private RemoteSigningConfiguration configuration;

  @BeforeEach
  void setUp() {
    clock = Clock.fixed(Instant.parse("2026-01-15T10:00:00Z"), ZoneOffset.UTC);
    configuration = configuration(Map.of(TEST_KEY_ID_1, TEST_SECRET_KEY_1), TEST_KEY_ID_1);
  }

  private static RemoteSigningConfiguration configuration(
      Map<String, String> keys, String currentKeyId) {
    RemoteSigningConfiguration configuration = mock(RemoteSigningConfiguration.class);
    when(configuration.keys()).thenReturn(keys);
    when(configuration.keyFiles()).thenReturn(Map.of());
    when(configuration.currentKeyId()).thenReturn(currentKeyId);
    when(configuration.tokenLifespan()).thenReturn(TOKEN_LIFESPAN);
    return configuration;
  }

  @Test
  void testConfigurationPropertyNames() {
    RemoteSigningConfiguration config =
        new SmallRyeConfigBuilder()
            .withMapping(RemoteSigningConfiguration.class)
            .addDiscoveredConverters()
            .withSources(
                new PropertiesConfigSource(
                    Map.of(
                        "polaris.remote-signing.keys.key1", TEST_SECRET_KEY_1,
                        "polaris.remote-signing.key-files.key2", "/path/to/key2",
                        "polaris.remote-signing.key-directory", "/path/to/keys",
                        "polaris.remote-signing.current-key-id", "key1",
                        "polaris.remote-signing.token-lifespan", "PT1H",
                        "polaris.remote-signing.decision-cache-ttl", "PT30S"),
                    "test"))
            .build()
            .getConfigMapping(RemoteSigningConfiguration.class);

    assertThat(config.keys()).containsExactly(Map.entry("key1", TEST_SECRET_KEY_1));
    assertThat(config.keyFiles()).containsExactly(Map.entry("key2", Path.of("/path/to/key2")));
    assertThat(config.keyDirectory()).contains(Path.of("/path/to/keys"));
    assertThat(config.currentKeyId()).isEqualTo("key1");
    assertThat(config.tokenLifespan()).isEqualTo(Duration.ofHours(1));
    assertThat(config.decisionCacheTtl()).isEqualTo(Duration.ofSeconds(30));
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testTokenRoundTrip(boolean stagedCreate) {
    RemoteSigningTokenService service = new RemoteSigningTokenService(configuration, clock);
    RemoteSigningContext context =
        new RemoteSigningContext(
            TEST_REALM,
            "test-catalog",
            TEST_TABLE,
            stagedCreate ? null : 42L,
            "load-1",
            Set.of(
                StorageLocation.of("s3://bucket/path1"), StorageLocation.of("s3://bucket/path2")),
            stagedCreate,
            !stagedCreate);

    String token = service.issueToken(context, TEST_PRINCIPAL);

    // A JWS has 3 parts separated by dots
    assertThat(token.split("\\.")).hasSize(3);
    assertThat(service.verifyToken(token, TEST_PRINCIPAL))
        .isEqualTo(new RemoteSigningTokenService.VerifiedToken(context, false));
  }

  @Test
  void testTokenDoesNotDiscloseThePrincipal() {
    RemoteSigningTokenService service = new RemoteSigningTokenService(configuration, clock);

    String token = service.issueToken(TEST_CONTEXT, TEST_PRINCIPAL);

    String payload =
        new String(Base64.getUrlDecoder().decode(token.split("\\.")[1]), StandardCharsets.UTF_8);
    assertThat(payload).contains("test-catalog").doesNotContain(TEST_PRINCIPAL);
  }

  @Test
  void testTokenForAnotherPrincipal() {
    RemoteSigningTokenService service = new RemoteSigningTokenService(configuration, clock);

    String token = service.issueToken(TEST_CONTEXT, TEST_PRINCIPAL);

    assertThatThrownBy(() -> service.verifyToken(token, "another-principal"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Token was issued to a different principal");
  }

  @Test
  void testTamperedToken() {
    RemoteSigningTokenService service = new RemoteSigningTokenService(configuration, clock);
    String token = service.issueToken(TEST_CONTEXT, TEST_PRINCIPAL);
    String otherToken =
        service.issueToken(
            new RemoteSigningContext(
                TEST_REALM,
                "test-catalog",
                TEST_TABLE,
                42L,
                "load-1",
                Set.of(StorageLocation.of("s3://other-bucket/")),
                false,
                true),
            TEST_PRINCIPAL);

    // The payload of one token, with the signature of the other
    String[] parts = token.split("\\.");
    String tampered = parts[0] + "." + otherToken.split("\\.")[1] + "." + parts[2];

    assertThatThrownBy(() -> service.verifyToken(tampered, TEST_PRINCIPAL))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Invalid token signature");
  }

  @Test
  void testInvalidToken() {
    RemoteSigningTokenService service = new RemoteSigningTokenService(configuration, clock);

    assertThatThrownBy(() -> service.verifyToken("invalid-token", TEST_PRINCIPAL))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Failed to parse remote signing token");
  }

  @Test
  void testExpiredToken() {
    RemoteSigningTokenService service = new RemoteSigningTokenService(configuration, clock);
    String token = service.issueToken(TEST_CONTEXT, TEST_PRINCIPAL);

    // 20 minutes later
    Clock expiredClock = Clock.fixed(Instant.parse("2026-01-15T10:20:00Z"), ZoneOffset.UTC);
    RemoteSigningTokenService expiredService =
        new RemoteSigningTokenService(configuration, expiredClock);

    // Expired tokens are reported as such, rather than rejected
    assertThat(expiredService.verifyToken(token, TEST_PRINCIPAL))
        .isEqualTo(new RemoteSigningTokenService.VerifiedToken(TEST_CONTEXT, true));
  }

  @ParameterizedTest
  @ValueSource(strings = {"PT0S", "-PT1H"})
  void testUnexpirableToken(Duration tokenLifespan) {
    when(configuration.tokenLifespan()).thenReturn(tokenLifespan);
    RemoteSigningTokenService service = new RemoteSigningTokenService(configuration, clock);
    String token = service.issueToken(TEST_CONTEXT, TEST_PRINCIPAL);

    // 24 years later
    Clock futureClock = Clock.fixed(Instant.parse("2050-01-15T10:00:00Z"), ZoneOffset.UTC);
    RemoteSigningTokenService futureService =
        new RemoteSigningTokenService(configuration, futureClock);

    assertThat(futureService.verifyToken(token, TEST_PRINCIPAL))
        .isEqualTo(new RemoteSigningTokenService.VerifiedToken(TEST_CONTEXT, false));
  }

  @Test
  void testUnexpirableTokenNotAllowed() {
    RemoteSigningConfiguration unexpirableConfig =
        configuration(Map.of(TEST_KEY_ID_1, TEST_SECRET_KEY_1), TEST_KEY_ID_1);
    when(unexpirableConfig.tokenLifespan()).thenReturn(Duration.ZERO);
    String unexpirableToken =
        new RemoteSigningTokenService(unexpirableConfig, clock)
            .issueToken(TEST_CONTEXT, TEST_PRINCIPAL);

    // A service whose tokens expire rejects tokens without an expiration time
    RemoteSigningTokenService service = new RemoteSigningTokenService(configuration, clock);
    assertThatThrownBy(() -> service.verifyToken(unexpirableToken, TEST_PRINCIPAL))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Token is missing expiration time");
  }

  @Test
  void testUnknownKeyId() {
    RemoteSigningTokenService service = new RemoteSigningTokenService(configuration, clock);
    String token = service.issueToken(TEST_CONTEXT, TEST_PRINCIPAL);

    // A service with a different key, simulating the removal of the original key
    RemoteSigningTokenService differentService =
        new RemoteSigningTokenService(
            configuration(Map.of("different-key", TEST_SECRET_KEY_2), "different-key"), clock);

    assertThatThrownBy(() -> differentService.verifyToken(token, TEST_PRINCIPAL))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unknown key ID: test-key-1");
  }

  @Test
  void testKeyRotation() {
    RemoteSigningTokenService service1 = new RemoteSigningTokenService(configuration, clock);
    String token = service1.issueToken(TEST_CONTEXT, TEST_PRINCIPAL);

    RemoteSigningTokenService service2 =
        new RemoteSigningTokenService(
            configuration(
                Map.of(TEST_KEY_ID_1, TEST_SECRET_KEY_1, TEST_KEY_ID_2, TEST_SECRET_KEY_2),
                TEST_KEY_ID_2),
            clock);

    // Tokens signed with the previous key remain valid
    assertThat(service2.verifyToken(token, TEST_PRINCIPAL).context()).isEqualTo(TEST_CONTEXT);
    // New tokens are signed with the current key, unknown to the previous service
    String newToken = service2.issueToken(TEST_CONTEXT, TEST_PRINCIPAL);
    assertThatThrownBy(() -> service1.verifyToken(newToken, TEST_PRINCIPAL))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Unknown key ID: " + TEST_KEY_ID_2);
  }

  @Test
  void testMissingKeysGeneratesRandomKey() {
    RemoteSigningConfiguration noKeyConfig = configuration(Map.of(), "default");
    RemoteSigningTokenService service = new RemoteSigningTokenService(noKeyConfig, clock);
    assertThat(service)
        .extracting("keys", map(String.class, SecretKey.class))
        .hasSize(1)
        .containsKey("default");
    assertThat(
            service
                .verifyToken(service.issueToken(TEST_CONTEXT, TEST_PRINCIPAL), TEST_PRINCIPAL)
                .context())
        .isEqualTo(TEST_CONTEXT);
  }

  @Test
  void testCurrentKeyIdNotFound() {
    RemoteSigningConfiguration badConfig =
        configuration(Map.of("key1", TEST_SECRET_KEY_1, "key2", TEST_SECRET_KEY_2), "nonexistent");
    assertThatThrownBy(() -> new RemoteSigningTokenService(badConfig, clock))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage(
            "Current key ID 'nonexistent' not found in configured keys. Available key IDs: key1, key2");
  }

  @ParameterizedTest
  @ValueSource(ints = {32, 48, 64})
  void testKeyLength(int keyLengthBytes) {
    byte[] keyBytes = new byte[keyLengthBytes];
    new SecureRandom().nextBytes(keyBytes);
    RemoteSigningTokenService service =
        new RemoteSigningTokenService(
            configuration(Map.of("key", Base64.getEncoder().encodeToString(keyBytes)), "key"),
            clock);

    String token = service.issueToken(TEST_CONTEXT, TEST_PRINCIPAL);
    assertThat(service.verifyToken(token, TEST_PRINCIPAL).context()).isEqualTo(TEST_CONTEXT);
  }

  @Test
  void testKeyTooShort() {
    byte[] keyBytes = new byte[31];
    new SecureRandom().nextBytes(keyBytes);
    RemoteSigningConfiguration config =
        configuration(
            Map.of("short-key", Base64.getEncoder().encodeToString(keyBytes)), "short-key");

    assertThatThrownBy(() -> new RemoteSigningTokenService(config, clock))
        .isInstanceOf(IllegalStateException.class)
        .hasMessage("Secret key 'short-key' must be at least 32 bytes long, but was 31 bytes.");
  }
}
