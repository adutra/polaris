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

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.MACSigner;
import com.nimbusds.jose.crypto.MACVerifier;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.text.ParseException;
import java.time.Clock;
import java.time.Instant;
import java.util.Base64;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import javax.crypto.Mac;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.polaris.core.catalog.PolarisCatalogHelpers;
import org.apache.polaris.core.storage.StorageLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Service for issuing and verifying remote signing tokens, as JWS (JSON Web Signature) tokens
 * signed with HMAC-SHA256.
 *
 * <p>A token records a {@link RemoteSigningContext}, and is bound to the principal it is issued to.
 * It is not encrypted: it only carries information that the client already knows, except for the
 * principal, which is recorded as an HMAC of its name.
 *
 * <p>Multiple keys can be configured to enable smooth key rotation. The current key (specified by
 * {@link RemoteSigningConfiguration#currentKeyId()}) is used for signing, while all configured keys
 * can be used for verification. This allows existing tokens to remain valid during key rotation.
 */
@ApplicationScoped
public class RemoteSigningTokenService {

  private static final Logger LOGGER = LoggerFactory.getLogger(RemoteSigningTokenService.class);

  private static final String CLAIM_REALM_ID = "r";
  private static final String CLAIM_CATALOG_NAME = "c";
  private static final String CLAIM_TABLE_IDENTIFIER = "t";
  private static final String CLAIM_LOCATIONS = "l";
  private static final String CLAIM_PRINCIPAL_HASH = "h";
  private static final String CLAIM_STAGED_CREATE = "sc";

  /** The minimum key length for signing tokens with HMAC-SHA256. */
  private static final int MIN_KEY_BYTES = 32;

  private final RemoteSigningConfiguration configuration;
  private final Clock clock;
  private final Map<String, SecretKey> keys;
  private final String currentKeyId;

  @Inject
  public RemoteSigningTokenService(RemoteSigningConfiguration configuration, Clock clock) {
    this.configuration = configuration;
    this.clock = clock;
    this.keys = initializeKeys(configuration);
    this.currentKeyId = configuration.currentKeyId();
  }

  /**
   * Issues a signed token recording the given table context, bound to the given principal.
   *
   * <p>The token is signed with the current key.
   *
   * @param context the table context to record
   * @param principalName the name of the principal the token is issued to
   * @return the signed JWS token string
   */
  public String issueToken(RemoteSigningContext context, String principalName) {
    try {
      Instant now = clock.instant();
      SecretKey key = keys.get(currentKeyId);

      JWTClaimsSet.Builder claimsBuilder =
          new JWTClaimsSet.Builder()
              .issueTime(Date.from(now))
              .jwtID(context.loadId())
              .claim(CLAIM_REALM_ID, context.realmId())
              .claim(CLAIM_CATALOG_NAME, context.catalogName())
              .claim(
                  CLAIM_TABLE_IDENTIFIER,
                  PolarisCatalogHelpers.tableIdentifierToList(context.tableIdentifier()))
              .claim(
                  CLAIM_LOCATIONS,
                  context.locations().stream()
                      .map(StorageLocation::toString)
                      .collect(Collectors.toList()))
              .claim(CLAIM_PRINCIPAL_HASH, principalHash(key, principalName));
      if (context.stagedCreate()) {
        claimsBuilder.claim(CLAIM_STAGED_CREATE, true);
      }
      if (configuration.tokenLifespan().isPositive()) {
        claimsBuilder.expirationTime(Date.from(now.plus(configuration.tokenLifespan())));
      }

      SignedJWT jws =
          new SignedJWT(
              new JWSHeader.Builder(JWSAlgorithm.HS256).keyID(currentKeyId).build(),
              claimsBuilder.build());
      jws.sign(new MACSigner(key));
      return jws.serialize();
    } catch (JOSEException e) {
      throw new IllegalArgumentException("Failed to create remote signing token", e);
    }
  }

  /**
   * Verifies a token issued by {@link #issueToken}, and returns the table context it records.
   *
   * <p>The key ID from the token header is validated against the configured keys. This ensures that
   * tokens signed with unknown keys are rejected.
   *
   * @param token the JWS token string
   * @param principalName the name of the authenticated principal, which must be the one the token
   *     was issued to
   * @return the table context
   * @throws IllegalArgumentException if the token is invalid, expired, signed with an unknown key,
   *     or issued to another principal
   */
  public RemoteSigningContext verifyToken(String token, String principalName) {
    try {
      SignedJWT jws = SignedJWT.parse(token);

      if (!JWSAlgorithm.HS256.equals(jws.getHeader().getAlgorithm())) {
        throw new IllegalArgumentException(
            "Unexpected token algorithm: " + jws.getHeader().getAlgorithm());
      }
      String keyId = jws.getHeader().getKeyID();
      if (keyId == null) {
        throw new IllegalArgumentException("Token is missing key ID header");
      }
      SecretKey key = keys.get(keyId);
      if (key == null) {
        throw new IllegalArgumentException("Unknown key ID: " + keyId);
      }
      if (!jws.verify(new MACVerifier(key))) {
        throw new IllegalArgumentException("Invalid token signature");
      }

      JWTClaimsSet claims = jws.getJWTClaimsSet();
      checkExpiration(claims);

      String principal = claims.getStringClaim(CLAIM_PRINCIPAL_HASH);
      if (principal == null
          || !MessageDigest.isEqual(
              principal.getBytes(StandardCharsets.UTF_8),
              principalHash(key, principalName).getBytes(StandardCharsets.UTF_8))) {
        throw new IllegalArgumentException("Token was issued to a different principal");
      }

      String realmId = claims.getStringClaim(CLAIM_REALM_ID);
      String catalogName = claims.getStringClaim(CLAIM_CATALOG_NAME);
      List<String> tableIdentifier = claims.getStringListClaim(CLAIM_TABLE_IDENTIFIER);
      List<String> locations = claims.getStringListClaim(CLAIM_LOCATIONS);
      Boolean stagedCreate = claims.getBooleanClaim(CLAIM_STAGED_CREATE);
      if (realmId == null
          || catalogName == null
          || tableIdentifier == null
          || locations == null
          || claims.getJWTID() == null) {
        throw new IllegalArgumentException("Token is missing required claims");
      }

      return new RemoteSigningContext(
          realmId,
          catalogName,
          TableIdentifier.of(tableIdentifier.toArray(new String[0])),
          claims.getJWTID(),
          locations.stream().map(StorageLocation::of).collect(Collectors.toSet()),
          Boolean.TRUE.equals(stagedCreate));

    } catch (ParseException | JOSEException e) {
      throw new IllegalArgumentException("Failed to parse remote signing token", e);
    }
  }

  private void checkExpiration(JWTClaimsSet claims) {
    Date expirationTime = claims.getExpirationTime();
    if (expirationTime == null) {
      if (configuration.tokenLifespan().isPositive()) {
        throw new IllegalArgumentException("Token is missing expiration time");
      }
    } else if (expirationTime.toInstant().isBefore(clock.instant())) {
      throw new IllegalArgumentException("Invalid or expired remote signing token");
    }
  }

  /**
   * Returns an HMAC-SHA256 of the principal name, so that tokens bind to a principal without
   * disclosing its name. The input is prefixed to separate it from the token signature.
   */
  private static String principalHash(SecretKey key, String principalName) {
    try {
      Mac mac = Mac.getInstance("HmacSHA256");
      mac.init(new SecretKeySpec(key.getEncoded(), "HmacSHA256"));
      byte[] hash = mac.doFinal(("principal:" + principalName).getBytes(StandardCharsets.UTF_8));
      return Base64.getUrlEncoder().withoutPadding().encodeToString(hash);
    } catch (GeneralSecurityException e) {
      throw new IllegalStateException("Failed to compute principal hash", e);
    }
  }

  private static Map<String, SecretKey> initializeKeys(RemoteSigningConfiguration configuration) {
    Map<String, String> inlineKeys = configuration.keys();
    Map<String, Path> keyFiles = configuration.keyFiles();
    Optional<Path> keyDirectory = configuration.keyDirectory();

    if (inlineKeys.isEmpty() && keyFiles.isEmpty() && keyDirectory.isEmpty()) {
      // Generate random secret key
      // (this should not happen in production, see ProductionReadinessChecks)
      LOGGER.warn(
          "No remote signing key configured, generating a random key. Remote signing tokens will "
              + "not be valid across server restarts or across multiple server replicas. Configure "
              + "keys with polaris.remote-signing.[keys|key-files|key-directory].");
      byte[] keyBytes = new byte[MIN_KEY_BYTES];
      new SecureRandom().nextBytes(keyBytes);
      return Map.of(configuration.currentKeyId(), new SecretKeySpec(keyBytes, "HmacSHA256"));
    }

    Map<String, SecretKey> keys = new HashMap<>();

    // Load inline keys
    for (Map.Entry<String, String> entry : inlineKeys.entrySet()) {
      keys.put(entry.getKey(), decodeKey(entry.getKey(), entry.getValue()));
    }

    // Load keys from files (these override inline keys if same key ID)
    for (Map.Entry<String, Path> entry : keyFiles.entrySet()) {
      String keyId = entry.getKey();
      keys.put(keyId, decodeKey(keyId, readKeyFromFile(keyId, entry.getValue())));
    }

    // Load keys from directory (these override inline and file-based keys if same key ID)
    if (keyDirectory.isPresent()) {
      Path dir = keyDirectory.get();
      if (!Files.isDirectory(dir)) {
        throw new IllegalStateException(
            "Key directory does not exist or is not a directory: " + dir);
      }
      try (var stream = Files.list(dir)) {
        stream
            .filter(Files::isRegularFile)
            .forEach(keyFile -> {
              String keyId = keyFile.getFileName().toString();
              keys.put(keyId, decodeKey(keyId, readKeyFromFile(keyId, keyFile)));
            });
      } catch (IOException e) {
        throw new IllegalStateException("Failed to read keys from directory: " + dir, e);
      }
    }

    String currentId = configuration.currentKeyId();
    if (!keys.containsKey(currentId)) {
      throw new IllegalStateException(
          "Current key ID '"
              + currentId
              + "' not found in configured keys. "
              + "Available key IDs: "
              + keys.keySet().stream().sorted().collect(Collectors.joining(", ")));
    }

    return Map.copyOf(keys);
  }

  private static String readKeyFromFile(String keyId, Path keyFile) {
    try {
      return Files.readString(keyFile).trim();
    } catch (IOException e) {
      throw new IllegalStateException(
          "Failed to read signing key '" + keyId + "' from file: " + keyFile, e);
    }
  }

  private static SecretKey decodeKey(String keyId, String base64Key) {
    byte[] keyBytes = Base64.getDecoder().decode(base64Key);
    if (keyBytes.length < MIN_KEY_BYTES) {
      throw new IllegalStateException(
          "Secret key '%s' must be at least %d bytes long, but was %d bytes."
              .formatted(keyId, MIN_KEY_BYTES, keyBytes.length));
    }
    return new SecretKeySpec(keyBytes, "HmacSHA256");
  }
}
