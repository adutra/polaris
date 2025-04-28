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
package org.apache.polaris.service.quarkus.auth.internal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.quarkus.security.AuthenticationFailedException;
import io.quarkus.security.identity.IdentityProviderManager;
import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.identity.request.TokenAuthenticationRequest;
import io.smallrye.mutiny.Uni;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.ext.web.RoutingContext;
import java.util.Optional;
import org.apache.polaris.service.auth.AuthenticationRealmConfiguration;
import org.apache.polaris.service.auth.AuthenticationType;
import org.apache.polaris.service.auth.DecodedToken;
import org.apache.polaris.service.auth.TokenBroker;
import org.apache.polaris.service.auth.TokenDecodeResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;

public class InternalAuthenticationMechanismTest {

  private InternalAuthenticationMechanism mechanism;
  private AuthenticationRealmConfiguration configuration;
  private TokenBroker tokenBroker;
  private IdentityProviderManager identityProviderManager;
  private RoutingContext routingContext;

  @BeforeEach
  public void setup() {
    configuration = mock(AuthenticationRealmConfiguration.class);
    tokenBroker = mock(TokenBroker.class);
    identityProviderManager = mock(IdentityProviderManager.class);
    routingContext = mock(RoutingContext.class);

    mechanism = new InternalAuthenticationMechanism();
    mechanism.configuration = configuration;
    mechanism.tokenBroker = tokenBroker;
  }

  @ParameterizedTest
  @CsvSource({
    "INTERNAL , true",
    "EXTERNAL , false",
    "MIXED    , true",
  })
  public void testShouldProcess(AuthenticationType type, boolean expectedResult) {
    when(configuration.type()).thenReturn(type);
    assertThat(
            mechanism.configuration.type() == AuthenticationType.INTERNAL
                || mechanism.configuration.type() == AuthenticationType.MIXED)
        .isEqualTo(expectedResult);
  }

  @Test
  public void testAuthenticateWithNoAuthHeader() {
    when(configuration.type()).thenReturn(AuthenticationType.INTERNAL);
    when(routingContext.request()).thenReturn(mock(HttpServerRequest.class));
    when(routingContext.request().getHeader("Authorization")).thenReturn(null);

    Uni<SecurityIdentity> result = mechanism.authenticate(routingContext, identityProviderManager);

    assertThat(result.await().indefinitely()).isNull();
    verify(tokenBroker, never()).decode(any());
  }

  @Test
  public void testAuthenticateWithInvalidAuthHeaderFormat() {
    when(configuration.type()).thenReturn(AuthenticationType.INTERNAL);
    when(routingContext.request()).thenReturn(mock(HttpServerRequest.class));
    when(routingContext.request().getHeader("Authorization")).thenReturn("InvalidFormat");

    Uni<SecurityIdentity> result = mechanism.authenticate(routingContext, identityProviderManager);

    assertThat(result.await().indefinitely()).isNull();
    verify(tokenBroker, never()).decode(any());
  }

  @Test
  public void testAuthenticateWithNonBearerAuthHeader() {
    when(configuration.type()).thenReturn(AuthenticationType.INTERNAL);
    when(routingContext.request()).thenReturn(mock(HttpServerRequest.class));
    when(routingContext.request().getHeader("Authorization")).thenReturn("Basic dXNlcjpwYXNz");

    Uni<SecurityIdentity> result = mechanism.authenticate(routingContext, identityProviderManager);

    assertThat(result.await().indefinitely()).isNull();
    verify(tokenBroker, never()).decode(any());
  }

  @Test
  public void testAuthenticateWithMalformedToken() {
    when(configuration.type()).thenReturn(AuthenticationType.MIXED);
    when(routingContext.request()).thenReturn(mock(HttpServerRequest.class));
    when(routingContext.request().getHeader("Authorization")).thenReturn("Bearer malformedToken");

    when(tokenBroker.decode("malformedToken"))
        .thenReturn(
            new TokenDecodeResult(TokenDecodeResult.Status.MALFORMED_TOKEN, Optional.empty()));

    SecurityIdentity securityIdentity = mock(SecurityIdentity.class);
    when(identityProviderManager.authenticate(any()))
        .thenReturn(Uni.createFrom().item(securityIdentity));

    Uni<SecurityIdentity> result = mechanism.authenticate(routingContext, identityProviderManager);

    assertThatThrownBy(() -> result.await().indefinitely())
        .isInstanceOf(AuthenticationFailedException.class)
        .hasMessage("Malformed token");
    verify(tokenBroker).decode("malformedToken");
    verify(identityProviderManager, never()).authenticate(any(TokenAuthenticationRequest.class));
  }

  @ParameterizedTest
  @EnumSource(
      value = TokenDecodeResult.Status.class,
      names = {"INVALID_ISSUER", "INVALID_REALM"})
  public void testAuthenticateWithInvalidTokenInternalAuth(TokenDecodeResult.Status status) {
    when(configuration.type()).thenReturn(AuthenticationType.INTERNAL);
    when(routingContext.request()).thenReturn(mock(HttpServerRequest.class));
    when(routingContext.request().getHeader("Authorization")).thenReturn("Bearer invalidToken");

    when(tokenBroker.decode("invalidToken"))
        .thenReturn(new TokenDecodeResult(status, Optional.empty()));

    SecurityIdentity securityIdentity = mock(SecurityIdentity.class);
    when(identityProviderManager.authenticate(any()))
        .thenReturn(Uni.createFrom().item(securityIdentity));

    Uni<SecurityIdentity> result = mechanism.authenticate(routingContext, identityProviderManager);

    assertThatThrownBy(() -> result.await().indefinitely())
        .isInstanceOf(AuthenticationFailedException.class)
        .hasMessage(status.message());
    verify(tokenBroker).decode("invalidToken");
    verify(identityProviderManager, never()).authenticate(any(TokenAuthenticationRequest.class));
  }

  @ParameterizedTest
  @EnumSource(
      value = TokenDecodeResult.Status.class,
      names = {"INVALID_ISSUER", "INVALID_REALM"})
  public void testAuthenticateWithInvalidTokenMixedAuth(TokenDecodeResult.Status status) {
    when(configuration.type()).thenReturn(AuthenticationType.MIXED);
    when(routingContext.request()).thenReturn(mock(HttpServerRequest.class));
    when(routingContext.request().getHeader("Authorization")).thenReturn("Bearer invalidToken");

    when(tokenBroker.decode("invalidToken"))
        .thenReturn(new TokenDecodeResult(status, Optional.empty()));

    SecurityIdentity securityIdentity = mock(SecurityIdentity.class);
    when(identityProviderManager.authenticate(any()))
        .thenReturn(Uni.createFrom().item(securityIdentity));

    Uni<SecurityIdentity> result = mechanism.authenticate(routingContext, identityProviderManager);

    assertThat(result.await().indefinitely()).isNull();
    verify(tokenBroker).decode("invalidToken");
    verify(identityProviderManager, never()).authenticate(any(TokenAuthenticationRequest.class));
  }

  @Test
  public void testAuthenticateWithValidToken() {
    when(configuration.type()).thenReturn(AuthenticationType.INTERNAL);
    when(routingContext.request()).thenReturn(mock(HttpServerRequest.class));
    when(routingContext.request().getHeader("Authorization")).thenReturn("Bearer validToken");

    DecodedToken decodedToken = mock(DecodedToken.class);
    when(tokenBroker.decode("validToken"))
        .thenReturn(
            new TokenDecodeResult(TokenDecodeResult.Status.SUCCESS, Optional.of(decodedToken)));

    SecurityIdentity securityIdentity = mock(SecurityIdentity.class);
    when(identityProviderManager.authenticate(any()))
        .thenReturn(Uni.createFrom().item(securityIdentity));

    Uni<SecurityIdentity> result = mechanism.authenticate(routingContext, identityProviderManager);

    assertThat(result.await().indefinitely()).isSameAs(securityIdentity);
    verify(tokenBroker).decode("validToken");
    verify(identityProviderManager).authenticate(any(TokenAuthenticationRequest.class));
  }
}
