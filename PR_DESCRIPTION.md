## Add support for S3 request signing

Fixes #32.

### Summary

This PR adds support for S3 remote request signing in Apache Polaris, allowing clients to request that Polaris sign S3 requests on their behalf instead of receiving temporary credentials. This is particularly useful for S3-compatible object storage systems that don't support STS (Security Token Service) for credential vending.

### What is Remote Signing?

Remote signing is an alternative to credential vending where:

1. Instead of receiving temporary AWS credentials, clients receive a signing endpoint URL
2. When the client needs to access S3, it sends the unsigned request to Polaris
3. Polaris signs the request using its own credentials and returns the signed request
4. The client uses the signed request to access S3 directly

This approach is useful when:

- Using S3-compatible storage systems (like MinIO) that don't support STS
- You want to avoid distributing temporary credentials to clients
- You need more fine-grained control over which requests are signed

### Key Features

- **New `remote-signing` access delegation mode**: Clients can request remote signing by setting the `X-Iceberg-Access-Delegation` header to `remote-signing`
- **New `TABLE_REMOTE_SIGN` privilege**: Controls which principals can use remote signing for a table
- **Secure token-based authorization**: Uses JWE (JSON Web Encryption) tokens to securely pass signing parameters between load table and sign request operations
- **Read/write authorization**: Separate authorization for read vs write operations based on `TABLE_READ_DATA` and `TABLE_WRITE_DATA` privileges
- **Configurable encryption**: Supports multiple encryption methods (AES-CBC, AES-GCM) with key rotation support
- **Reverse proxy support**: Properly handles `X-Forwarded-Prefix` headers for deployments behind reverse proxies

### Configuration

Remote signing is disabled by default. To enable:

1. Set the feature flag:
   - System-wide: `polaris.features."REMOTE_SIGNING_ENABLED"=true`
   - Per-catalog: Set `polaris.config.remote-signing.enabled=true` in catalog properties

2. Configure encryption keys (optional; a random key is generated if not provided):
   ```properties
   polaris.remote-signing.current-key-id=default
   polaris.remote-signing.keys.default=<base64-encoded-64-byte-key>
   # Or from files:
   polaris.remote-signing.key-files.key1=/etc/polaris/secrets/signing-key
   # Or from a directory:
   polaris.remote-signing.key-directory=/etc/polaris/secrets/signing-keys
   ```

3. Grant the `TABLE_REMOTE_SIGN` privilege to catalog roles, along with `TABLE_READ_DATA` and/or `TABLE_WRITE_DATA`

### Changes

#### New Components

- `RemoteSigningConfiguration`: Configuration interface for encryption keys and token settings
- `RemoteSigningTokenService`: Creates and validates JWE tokens for secure parameter passing
- `S3RemoteSigner`: Signs S3 requests using AWS SDK v2's `AwsV4HttpSigner`
- `S3StorageUriTranslator`: Translates from HTTP URIs to S3 URIs for authz checks during signing

#### Modified Components

- `PolarisPrivilege`: Added `TABLE_REMOTE_SIGN` privilege
- `PolarisAuthorizableOperation`: Added `REMOTE_SIGN` operation (requires `TABLE_REMOTE_SIGN`)
- `StorageAccessConfig`: Extended to support remote signing properties
- `IcebergCatalogHandler`: Added `signRequest` method; added remote signing support in `loadTable` and `createTable` (both direct and staged)
- `ProductionReadinessChecks`: Validates remote signing configuration for production deployments

#### Tests

- `RemoteSigningTokenServiceTest`: Unit tests for token encryption/decryption
- `S3RemoteSignerTest`: Unit tests for the S3 signer
- `S3StorageUriTranslatorTest`: Unit tests for URI translation
- `RemoteSigningCatalogHandlerAuthzTest`: Authorization tests for remote signing (Quarkus test)
- `PolarisRemoteSigningS3IntegrationTest`: Comprehensive integration tests for remote signing (Quarkus test)
- `RemoteSigningS3MinIOIT`: Integration tests using MinIO (Quarkus IT)

### Limitations

- Currently only supports S3 and S3-compatible storage (GCS and Azure support can be added later)
- Remote signing is not supported for external catalogs (could be enabled later)
- Cannot use remote signing when registering tables (only for loading existing tables)

### Security Considerations

- Remote signing tokens are encrypted using JWE with configurable encryption methods
- Tokens include allowed locations to prevent signing requests for unauthorized paths
- Read/write permissions are enforced at signing time
- Token expiration is configurable (default: 7 hours)
- Production deployments should configure explicit encryption keys rather than relying on auto-generated keys
