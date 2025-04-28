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

import io.quarkus.security.AuthenticationFailedException;
import io.quarkus.security.identity.AuthenticationRequestContext;
import io.quarkus.security.identity.IdentityProvider;
import io.quarkus.security.identity.SecurityIdentity;
import io.quarkus.security.identity.request.TokenAuthenticationRequest;
import io.quarkus.security.runtime.QuarkusSecurityIdentity;
import io.quarkus.vertx.http.runtime.security.HttpSecurityUtils;
import io.smallrye.mutiny.Uni;
import io.vertx.ext.web.RoutingContext;
import jakarta.annotation.Nullable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.security.Principal;
import java.util.Set;
import org.apache.polaris.service.auth.DecodedToken;
import org.apache.polaris.service.auth.TokenBroker;
import org.apache.polaris.service.quarkus.auth.QuarkusPrincipalAuthInfo;
import org.apache.polaris.service.quarkus.auth.internal.InternalAuthenticationMechanism.InternalTokenCredential;

/** A custom {@link IdentityProvider} that handles internal token authentication requests. */
@ApplicationScoped
public class InternalIdentityProvider implements IdentityProvider<TokenAuthenticationRequest> {

  @Inject TokenBroker tokenBroker;

  @Override
  public Class<TokenAuthenticationRequest> getRequestType() {
    return TokenAuthenticationRequest.class;
  }

  @Override
  public Uni<SecurityIdentity> authenticate(
      TokenAuthenticationRequest request, AuthenticationRequestContext context) {
    if (!(request.getToken() instanceof InternalTokenCredential credential)) {
      return Uni.createFrom().nullItem();
    }
    return Uni.createFrom().item(() -> verifyToken(credential, request));
  }

  private SecurityIdentity verifyToken(
      InternalTokenCredential credential, TokenAuthenticationRequest request) {
    DecodedToken verified;
    try {
      verified = tokenBroker.verify(credential.getDecodedToken());
    } catch (Exception e) {
      throw new AuthenticationFailedException(e);
    }
    return QuarkusSecurityIdentity.builder()
        .setPrincipal(new InternalTokenPrincipal(verified.getSub()))
        .addCredential(new InternalPrincipalAuthInfo(verified))
        .addAttribute(
            RoutingContext.class.getName(), HttpSecurityUtils.getRoutingContextAttribute(request))
        .build();
  }

  private record InternalTokenPrincipal(String getName) implements Principal {}

  static class InternalPrincipalAuthInfo implements QuarkusPrincipalAuthInfo {

    private final DecodedToken token;

    InternalPrincipalAuthInfo(DecodedToken token) {
      this.token = token;
    }

    @Nullable
    @Override
    public Long getPrincipalId() {
      return token.getPrincipalId();
    }

    @Nullable
    @Override
    public String getPrincipalName() {
      return token.getPrincipalName();
    }

    @Override
    public Set<String> getPrincipalRoles() {
      return token.getPrincipalRoles();
    }
  }
}
