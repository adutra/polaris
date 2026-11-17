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

package org.apache.polaris.core.storage;

/**
 * Properties that Polaris passes down to the Iceberg remote signer client via the {@code
 * remote-signing-config} field of the {@code LoadTableResult}, and that the client must send back,
 * unchanged, in the {@code properties} field of every {@code RemoteSignRequest}.
 */
public enum RemoteSigningProperty {

  /** The encrypted remote signing token, containing authorization information. Required. */
  TOKEN("polaris.remote-signing.token");

  private final String propertyName;

  RemoteSigningProperty(String propertyName) {
    this.propertyName = propertyName;
  }

  /** The property name as it appears in remote signing configs and signing requests. */
  public String propertyName() {
    return propertyName;
  }
}
