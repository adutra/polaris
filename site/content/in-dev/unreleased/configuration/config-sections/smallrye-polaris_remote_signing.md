---
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
title: smallrye-polaris_remote_signing
build:
  list: never
  render: never
---

Configuration for remote signing. 

Each sign request is authorized individually, against the caller's current privileges. When a  table is loaded, Polaris issues a remote signing token, which the client sends back with each  sign request. The token carries the table context only (catalog, table, locations and a binding  to the principal), and is signed with HMAC-SHA256 rather than encrypted. Clients older than  Iceberg 1.12 do not send the token back: they are supported too, except for tables created in  staged create transactions (e.g. CREATE TABLE AS SELECT).

| Property | Default Value | Type | Description |
|----------|---------------|------|-------------|
| `polaris.remote-signing.decision-cache-ttl` | `PT0S` | `duration` | The duration for which each Polaris node caches the outcome of a sign request authorization,  per realm, principal, catalog, table and access mode.  Grant revocations take effect after at  most this duration. The default, zero, disables the cache.  |
| `polaris.remote-signing.keys.`_`<name>`_ |  | `string` | Map of key IDs to their base64-encoded secret keys. These keys are used for signing and  verifying remote signing tokens with HMAC-SHA256, and must be at least 256 bits long.   <br><br>Multiple keys can be configured to enable smooth key rotation. The (`#currentKeyId()`) specifies which key is used for signing, while all configured keys can be used for  verification.   <br><br>For production use, consider using (`#keyFiles()`) instead to avoid exposing secrets in  configuration.   <br><br>Example of configuration:   <br><br>```<br> polaris.remote-signing.keys.key1=base64EncodedKey1<br> polaris.remote-signing.keys.key2=base64EncodedKey2<br> ```<br><br> |
| `polaris.remote-signing.key-files.`_`<name>`_ |  | `path` | Map of key IDs to file paths containing base64-encoded secret keys. These keys are used for  signing and verifying remote signing tokens with HMAC-SHA256.   <br><br>Each file should contain a single base64-encoded key that, when decoded, is at least 256  bits long. The file content is read at startup and trimmed of leading/trailing whitespace.   <br><br>Multiple keys can be configured to enable smooth key rotation. The (`#currentKeyId()`) specifies which key is used for signing, while all configured keys can be used for  verification.   <br><br>This is the preferred method for production deployments as it avoids exposing secrets in  configuration files or environment variables.   <br><br>Example of configuration:   <br><br>```<br> polaris.remote-signing.key-files.key1=/etc/polaris/secrets/signing-key1<br> polaris.remote-signing.key-files.key2=/etc/polaris/secrets/signing-key2<br> ```<br><br> |
| `polaris.remote-signing.key-directory` |  | `path` | Path to a directory containing key files. Each file in the directory represents a signing key  where the filename is used as the key ID and the file content is the base64-encoded secret key.   <br><br>Each file should contain a single base64-encoded key that, when decoded, is at least 256  bits long. The file content is read at startup and trimmed of leading/trailing whitespace.   <br><br>This is particularly useful for Kubernetes deployments where secrets can be mounted as files  in a volume directory.   <br><br>Keys loaded from this directory will override keys with the same ID from (`#keys()`) or  (`#keyFiles()`).   <br><br>Example of configuration:   <br><br>```<br> polaris.remote-signing.key-directory=/etc/polaris/secrets/signing-keys/<br> ```<br><br>With files:   <br><br>```<br> /etc/polaris/secrets/signing-keys/key1  (content: base64 encoded key)<br> /etc/polaris/secrets/signing-keys/key2  (content: base64 encoded key)<br> ```<br><br> |
| `polaris.remote-signing.current-key-id` | `default` | `string` | The key ID to use for signing new tokens. <br><br>This key ID must exist in either the (`#keys()`) or (`#keyFiles()`) map. During key  rotation, update this value to the new key ID after adding the new key.  |
| `polaris.remote-signing.token-lifespan` | `PT24H` | `duration` | The duration for which remote signing tokens are valid. The default is 24 hours.   <br><br>After this duration, sign requests carrying the token will be rejected.   <br><br>If this value is set to zero or negative, tokens will be created without an expiration time. |
