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
title: Remote Signing
linkTitle: Remote Signing
type: docs
weight: 465
---

{{< alert warning >}}
Remote signing is an experimental feature. It is disabled by default, and should not be enabled in
production yet.
{{< /alert >}}

With remote signing, clients never receive storage credentials. Instead, they ask Polaris to sign
each request to the object store, and Polaris signs it with its own credentials, after checking
that the caller is allowed to make it. Compared to [vended credentials]({{% relref
"./vended-credentials" %}}), access is decided for each request, against the caller's current
privileges, and is limited to the locations of the table, whatever the object store supports for
scoping credentials.

Remote signing is only supported for tables stored on S3 or an S3-compatible object store, in
internal catalogs.

## How it works

1. The client loads or creates a table with the `X-Iceberg-Access-Delegation: remote-signing`
   header. Polaris checks that remote signing is enabled for the catalog and that the caller holds
   the `TABLE_REMOTE_SIGN` privilege, then returns the table with a remote signing token in the
   `remote-signing-config` field of the `LoadTableResult`, introduced in Iceberg 1.12.
2. Before each request to the object store, the client sends the request to sign to the signing
   endpoint of the table, `/v1/{prefix}/namespaces/{namespace}/tables/{table}/sign`, along with
   the token.
3. Polaris authorizes the request against the caller's current privileges on the table, checks
   that the request only accesses objects at the locations of the table, then signs it.
4. The client sends the signed request to the object store.

Requests for a table that exists are always authorized against the caller's current privileges,
whether they carry a token or not. The token is only used for a table that does not exist: a table
being created in a staged create transaction, or a table dropped since the token was issued. It
records the table that was loaded, its locations, the principal that loaded it and, for dropped
tables, whether that principal was allowed to drop the table with purge.

## Enabling remote signing

Remote signing is enabled with the realm-wide feature `REMOTE_SIGNING_ENABLED`, or for one catalog
with the catalog property `polaris.config.remote-signing.enabled`. External catalogs do not
support it.

Tokens are signed with HMAC-SHA256, using secret keys of at least 256 bits configured with
`polaris.remote-signing.keys`, `polaris.remote-signing.key-files` or
`polaris.remote-signing.key-directory`. All Polaris nodes of a deployment must share the same keys.
If no key is configured, each node generates a random one, so that tokens are only valid on the node
that issued them, until it restarts. The production readiness checks report this configuration.

Several keys can be configured to rotate them: `polaris.remote-signing.current-key-id` selects the
key that signs new tokens, while all configured keys verify them. Tokens are valid for 24 hours by
default (`polaris.remote-signing.token-lifespan`). See the [configuration reference]({{% relref
"./configuration/config-sections/smallrye-polaris_remote_signing" %}}) for all settings.

### Storage configuration

Polaris only signs requests that target the S3 endpoint of the catalog's storage configuration: its
`endpoint` if set, or AWS S3 endpoints otherwise. If clients access S3 through another endpoint,
such as a VPC interface endpoint or an S3-compatible object store, that endpoint must be set in the
storage configuration, along with `pathStyleAccess` if the object store requires it.

## Privileges

| Request                                         | Privileges required on the table                       |
|-------------------------------------------------|--------------------------------------------------------|
| Loading, creating or registering a table        | `TABLE_REMOTE_SIGN`, besides those of the operation     |
| Signing a read request (`GET`, `HEAD`)          | `TABLE_REMOTE_SIGN` and `TABLE_READ_DATA`               |
| Signing a write or delete request               | `TABLE_REMOTE_SIGN` and `TABLE_WRITE_DATA`              |

`TABLE_REMOTE_SIGN` is not included in `TABLE_FULL_METADATA`, and must be granted along with
`TABLE_READ_DATA` or `TABLE_WRITE_DATA`. `CATALOG_MANAGE_CONTENT` includes all of them.

A table created in a staged create transaction, such as `CREATE TABLE AS SELECT`, only exists once
the transaction commits. Until then, signing requests for it require `TABLE_CREATE`,
`TABLE_WRITE_DATA` and `TABLE_REMOTE_SIGN` on its namespace. Once it exists, they are authorized
against the table, like any other.

When a table is loaded, created or registered, Polaris also checks whether the caller is allowed
to drop it with purge (`TABLE_DROP` and `TABLE_WRITE_DATA`), and records the outcome in the token.
This decision authorizes reading and deleting the objects of the table once it is dropped, as
described in [dropped tables](#dropped-tables).

External authorizers see the operations `REMOTE_SIGN` and `DROP_TABLE_WITH_PURGE` (when a table is
loaded, created or registered), `REMOTE_SIGN_READ`, `REMOTE_SIGN_WRITE` and
`REMOTE_SIGN_STAGED_CREATE`. With the Ranger authorizer, the access type
corresponding to `TABLE_REMOTE_SIGN` is `table-remote-sign`, which must be added to the Polaris
service definition in Ranger.

### Decision cache

Each signing request is authorized individually, which requires resolving the table and the
caller's grants. To reduce this cost, each Polaris node can cache the outcome of authorizations for
a short time, set with `polaris.remote-signing.decision-cache-ttl`. The cache is disabled by
default. When it is enabled, revoked privileges keep allowing signing requests for at most this
duration.

## Requests that Polaris signs

Polaris signs the requests that Iceberg clients send to read, write and delete table files:

- getting, heading, putting and deleting an object;
- creating, uploading parts of, completing and aborting a multipart upload;
- deleting several objects at once (`DeleteObjects`), in which case each object must be at the
  locations of the table.

Other requests are refused, notably those that list objects, copy objects, change access control
lists, tags, object locks or retention settings, or access specific versions of objects. Headers
are refused likewise, except those needed by the requests above, such as checksums, server-side
encryption settings, storage classes and user metadata.

Clients are not allowed to cache signed requests.

## Client support

Clients must support the `remote-signing-config` field to use all features of remote signing.

| Client                                                            | Support                                                                                      |
|-------------------------------------------------------------------|----------------------------------------------------------------------------------------------|
| Iceberg Java 1.12 or later (Spark, Flink, etc.)                   | Full                                                                                         |
| Iceberg Java before 1.12                                          | No staged creates, and no access to dropped tables, see [limitations](#limitations)          |
| PyIceberg, with its fsspec FileIO                                 | Same as Iceberg Java before 1.12                                                             |
| Trino, iceberg-rust, iceberg-go, and other clients                | None                                                                                         |

Older Iceberg Java clients, and PyIceberg, do not send the token back. For them, Polaris also returns
the `s3.signer.endpoint` property, which tells them where to send signing requests, and the
`s3.signer` property, which enables PyIceberg's signer. PyIceberg only supports remote signing with
its fsspec-based FileIO, which must be selected with `py-io-impl=pyiceberg.io.fsspec.FsspecFileIO`.

Trino accesses S3 through its own file system, which does not implement remote signing, whatever
the version of the Iceberg library it uses.

## Limitations

### Clients without a token

Requests without a token are authorized against the table as it is stored. As a result, clients
that do not send the token back cannot:

- create tables in staged create transactions, such as `CREATE TABLE AS SELECT` or
  `REPLACE TABLE AS SELECT`, as the table does not exist before the transaction commits;
- access the objects of a dropped table, e.g. to purge it.

### Dropped tables

Clients may access the objects of a table after dropping it: Spark, for example, implements
`DROP TABLE ... PURGE` by dropping the table, then deleting its files. Polaris signs such requests
if all of the following conditions are met:

- the request carries a token that has not expired, issued to the same principal when it loaded the
  table;
- the principal was allowed to drop the table with purge when it loaded the table;
- the request only reads or deletes objects, at the locations of the table when it was loaded;
- the table was dropped, not renamed;
- no table or view has been created with the same name since, and no table or namespace uses a
  location that overlaps the table's locations. This last check is skipped if
  `ALLOW_TABLE_LOCATION_OVERLAP` is enabled.

The grants on a table are dropped with it, so these requests are authorized by the decision
recorded in the token, which the authorizer took when the table was loaded: a privilege revoked
after the table was loaded still applies to its objects until the token expires. Spark loads the
table right before dropping and purging it, so its tokens are recent.

### Token expiration

Tokens expire after `polaris.remote-signing.token-lifespan` (24 hours by default). An expired
token does not prevent signing requests for a table that exists, as they are authorized against
the caller's current privileges anyway: long-running jobs can keep using a table for longer than
the token lifespan. Requests that rely on the token, for staged creates and dropped tables, are
refused once it expired.

### Renamed tables

The signing endpoint, and the token, name the table that was loaded. After a table is renamed,
signing requests for its former name are refused, so clients must load the table again.

### Unsupported operations

Since objects cannot be listed, removing orphan files does not work with remote signing. Client
options that rely on refused headers or requests are not supported either, such as access control
lists (`s3.acl`) and tags (`s3.write.tags`, `s3.delete.tags`) in Iceberg's `S3FileIO`.
