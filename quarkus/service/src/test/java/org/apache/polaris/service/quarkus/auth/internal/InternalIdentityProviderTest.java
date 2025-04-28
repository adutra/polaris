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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.quarkus.security.AuthenticationFailedException;
import io.quarkus.security.credential.TokenCredential;
import io.quarkus.security.identity.AuthenticationRequestContext;
import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.identity.request.TokenAuthenticationRequest;
import io.quarkus.vertx.http.runtime.security.HttpSecurityUtils;
import io.smallrye.mutiny.Uni;
import io.vertx.ext.web.RoutingContext;
import java.security.Principal;
import java.util.Set;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.polaris.service.auth.DecodedToken;
import org.apache.polaris.service.auth.TokenBroker;
import org.apache.polaris.service.quarkus.auth.QuarkusPrincipalAuthInfo;
import org.apache.polaris.service.quarkus.auth.internal.InternalAuthenticationMechanism.InternalTokenCredential;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class InternalIdentityProviderTest {

  private InternalIdentityProvider provider;
  private AuthenticationRequestContext context;
  private TokenBroker tokenBroker;

  @BeforeEach
  public void setup() {
    provider = new InternalIdentityProvider();
    context = mock(AuthenticationRequestContext.class);
    tokenBroker = mock(TokenBroker.class);
    provider.tokenBroker = tokenBroker;
  }

  @Test
  public void testAuthenticateWithWrongCredential() {
    TokenCredential nonInternalCredential = mock(TokenCredential.class);
    TokenAuthenticationRequest request = new TokenAuthenticationRequest(nonInternalCredential);

    Uni<SecurityIdentity> result = provider.authenticate(request, context);

    assertThat(result.await().indefinitely()).isNull();
  }

  @Test
  public void testAuthenticateWithInvalidCredential() {
    // Create a mock InternalTokenCredential
    InternalTokenCredential credential = mock(InternalTokenCredential.class);
    DecodedToken token = mock(DecodedToken.class);
    when(credential.getDecodedToken()).thenReturn(token);
    when(tokenBroker.verify(token)).thenThrow(new NotAuthorizedException("Invalid token"));

    // Create a request with the credential
    TokenAuthenticationRequest request = new TokenAuthenticationRequest(credential);

    // Authenticate the request
    Uni<SecurityIdentity> result = provider.authenticate(request, context);

    // Verify the result
    assertThatThrownBy(() -> result.await().indefinitely())
        .isInstanceOf(AuthenticationFailedException.class)
        .hasCauseInstanceOf(NotAuthorizedException.class);
  }

  @Test
  public void testAuthenticateWithValidCredential() {
    // Create a mock InternalTokenCredential
    InternalTokenCredential credential = mock(InternalTokenCredential.class);
    DecodedToken token = mock(DecodedToken.class);
    when(token.getPrincipalId()).thenReturn(123L);
    when(token.getSub()).thenReturn("123");
    when(token.getPrincipalRoles()).thenReturn(Set.of("role1", "role2"));
    when(credential.getDecodedToken()).thenReturn(token);
    when(tokenBroker.verify(token)).thenReturn(token);

    // Create a request with the credential and a routing context attribute
    RoutingContext routingContext = mock(RoutingContext.class);
    TokenAuthenticationRequest request = new TokenAuthenticationRequest(credential);
    HttpSecurityUtils.setRoutingContextAttribute(request, routingContext);

    // Authenticate the request
    Uni<SecurityIdentity> result = provider.authenticate(request, context);

    // Verify the result
    SecurityIdentity identity = result.await().indefinitely();
    assertThat(identity).isNotNull();

    // Verify the principal
    Principal principal = identity.getPrincipal();
    assertThat(principal).isNotNull();
    assertThat(principal.getName()).isEqualTo("123");

    // Verify the credential
    QuarkusPrincipalAuthInfo authInfo = identity.getCredential(QuarkusPrincipalAuthInfo.class);
    assertThat(authInfo).isNotNull();
    assertThat(authInfo.getPrincipalName()).isNull(); // not set by Polaris tokens
    assertThat(authInfo.getPrincipalId()).isEqualTo(123L);
    assertThat(authInfo.getPrincipalRoles()).containsExactlyInAnyOrder("role1", "role2");

    // Verify the routing context attribute is set
    assertThat((RoutingContext) identity.getAttribute(RoutingContext.class.getName()))
        .isSameAs(routingContext);
  }
}
