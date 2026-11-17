### New Features

- Added experimental support for S3 remote signing (`X-Iceberg-Access-Delegation: remote-signing`), disabled
  by default. Each signing request is authorized against the caller's current privileges. Polaris communicates
  the signer settings to clients through the `remote-signing-config` field of the `LoadTableResult` introduced in
  Iceberg 1.12, which the Iceberg Java client 1.12 or later supports. Older Iceberg Java clients and PyIceberg
  (with its fsspec FileIO) are also supported, with limitations. See the Remote Signing page of the documentation
  for client support and limitations. A new table privilege, `TABLE_REMOTE_SIGN`, was introduced. To enable
  remote signing:
    1. Set the realm-wide feature `REMOTE_SIGNING_ENABLED` or the catalog-level property
       `polaris.config.remote-signing.enabled` to `true`.
    2. Grant the `TABLE_REMOTE_SIGN` privilege to a catalog role. The catalog role must also be
       granted `TABLE_READ_DATA` (for reads) and/or `TABLE_WRITE_DATA` (for writes).

  With the Ranger authorizer, the corresponding access type is `table-remote-sign`, which must be added to the
  Polaris service definition in Ranger, along with `table-data-read` and/or `table-data-write`.

  Clients are not allowed to cache signed requests.
