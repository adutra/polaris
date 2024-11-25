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
package org.apache.polaris.service.config;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithParentName;
import jakarta.annotation.Nullable;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.polaris.core.PolarisCallContext;
import org.apache.polaris.core.PolarisConfigurationStore;
import org.apache.polaris.core.context.CallContext;

@ApplicationScoped
public class DefaultConfigurationStore implements PolarisConfigurationStore {

  private final Map<String, Object> defaults;
  private final Map<String, Map<String, Object>> realmOverrides;

  // FIXME the whole PolarisConfigurationStore + PolarisConfiguration needs to be refactored
  // to become a proper Quarkus configuration object
  @Inject
  public DefaultConfigurationStore(
      ObjectMapper objectMapper, FeatureConfigurations configurations) {
    this(
        configurations.parseDefaults(objectMapper),
        configurations.parseRealmOverrides(objectMapper));
  }

  public DefaultConfigurationStore(Map<String, Object> defaults) {
    this(defaults, Map.of());
  }

  public DefaultConfigurationStore(
      Map<String, Object> defaults, Map<String, Map<String, Object>> realmOverrides) {
    this.defaults = Map.copyOf(defaults);
    this.realmOverrides = Map.copyOf(realmOverrides);
  }

  @Override
  public <T> @Nullable T getConfiguration(PolarisCallContext ctx, String configName) {
    String realm = CallContext.getCurrentContext().getRealmContext().getRealmIdentifier();
    @SuppressWarnings("unchecked")
    T confgValue =
        (T)
            realmOverrides
                .getOrDefault(realm, Map.of())
                .getOrDefault(configName, defaults.get(configName));
    return confgValue;
  }

  @ConfigMapping(prefix = "polaris.config")
  public interface FeatureConfigurations {

    Map<String, String> defaults();

    Map<String, RealmOverrides> realmOverrides();

    default Map<String, Object> parseDefaults(ObjectMapper objectMapper) {
      return convertMap(objectMapper, defaults());
    }

    default Map<String, Map<String, Object>> parseRealmOverrides(ObjectMapper objectMapper) {
      Map<String, Map<String, Object>> m = new HashMap<>();
      for (String realm : realmOverrides().keySet()) {
        m.put(realm, convertMap(objectMapper, realmOverrides().get(realm).overrides()));
      }
      return m;
    }
  }

  public interface RealmOverrides {
    @WithParentName
    Map<String, String> overrides();
  }

  private static Map<String, Object> convertMap(
      ObjectMapper objectMapper, Map<String, String> properties) {
    Map<String, Object> m = new HashMap<>();
    for (String configName : properties.keySet()) {
      String json = properties.get(configName);
      try {
        JsonNode node = objectMapper.readTree(json);
        m.put(configName, configValue(node));
      } catch (JsonProcessingException e) {
        throw new RuntimeException(
            "Invalid JSON value for feature configuration: " + configName, e);
      }
    }
    return m;
  }

  private static Object configValue(JsonNode node) {
    return switch (node.getNodeType()) {
      case BOOLEAN -> node.asBoolean();
      case STRING -> node.asText();
      case NUMBER ->
          switch (node.numberType()) {
            case INT, LONG -> node.asLong();
            case FLOAT, DOUBLE -> node.asDouble();
            default ->
                throw new IllegalArgumentException("Unsupported number type: " + node.numberType());
          };
      case ARRAY -> {
        List<Object> list = new ArrayList<>();
        node.elements().forEachRemaining(n -> list.add(configValue(n)));
        yield List.copyOf(list);
      }
      default ->
          throw new IllegalArgumentException(
              "Unsupported feature configuration JSON type: " + node.getNodeType());
    };
  }
}
