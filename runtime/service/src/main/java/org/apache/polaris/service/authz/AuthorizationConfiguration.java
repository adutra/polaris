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

import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithDefault;
import java.util.Map;
import org.apache.polaris.core.context.RealmContext;
import org.apache.polaris.docs.ConfigDocs;

@ConfigMapping(prefix = "polaris.authorization")
public interface AuthorizationConfiguration {

  /**
   * The global default authorizer type. Must be a registered {@link
   * org.apache.polaris.core.auth.PolarisAuthorizerFactory} identifier. Can be overridden per realm
   * via {@link #realms()}.
   */
  @WithDefault("internal")
  String type();

  /**
   * Per-realm authorizer type overrides. When a realm is present in this map, its type takes
   * precedence over the global {@link #type()}. Configuration prefix:
   * {@code polaris.authorization.realms.<realm-id>.type}.
   */
  @ConfigDocs.ConfigPropertyName("realm")
  Map<String, AuthorizationRealmConfiguration> realms();

  default AuthorizationRealmConfiguration forRealm(RealmContext realmContext) {
    return forRealm(realmContext.getRealmIdentifier());
  }

  default AuthorizationRealmConfiguration forRealm(String realmIdentifier) {
    if (realms().containsKey(realmIdentifier)) {
      return realms().get(realmIdentifier);
    }
    String globalType = type();
    return () -> globalType;
  }
}
