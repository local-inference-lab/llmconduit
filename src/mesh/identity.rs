use iroh::Endpoint;
use iroh::EndpointId;
use iroh::RelayMode;
use iroh::SecretKey;
use iroh::endpoint::presets;
use std::io;
use std::path::Path;
use std::path::PathBuf;
use tokio::fs;
use uuid::Uuid;

const SECRET_KEY_BYTES: usize = 32;

#[derive(Clone)]
pub struct MeshIdentity {
    secret_key: SecretKey,
}

impl std::fmt::Debug for MeshIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MeshIdentity")
            .field("endpoint_id", &self.endpoint_id().to_string())
            .finish()
    }
}

impl MeshIdentity {
    pub fn secret_key(&self) -> SecretKey {
        self.secret_key.clone()
    }

    pub fn endpoint_id(&self) -> EndpointId {
        self.secret_key.public()
    }

    pub async fn bind_direct_endpoint(
        &self,
        alpns: Vec<Vec<u8>>,
    ) -> Result<Endpoint, iroh::endpoint::BindError> {
        Endpoint::builder(presets::Minimal)
            .secret_key(self.secret_key())
            .relay_mode(RelayMode::Disabled)
            .alpns(alpns)
            .bind()
            .await
    }
}

pub async fn load_or_create(path: impl AsRef<Path>) -> io::Result<MeshIdentity> {
    let path = path.as_ref();
    match fs::read(path).await {
        Ok(bytes) => {
            let identity = parse_identity_bytes(&bytes)?;
            restrict_existing_secret_file(path).await?;
            return Ok(identity);
        }
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }

    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        create_private_dir_all(parent).await?;
    }

    let secret_key = SecretKey::generate();
    let encoded = hex::encode(secret_key.to_bytes());
    let tmp_path = temp_path(path);
    write_new_secret_file(&tmp_path, encoded.as_bytes()).await?;
    // Linking a fully-synced temporary file publishes it atomically without
    // replacing an identity another process may have created concurrently.
    match fs::hard_link(&tmp_path, path).await {
        Ok(()) => {
            let _ = fs::remove_file(&tmp_path).await;
            Ok(MeshIdentity { secret_key })
        }
        Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
            let _ = fs::remove_file(&tmp_path).await;
            let bytes = fs::read(path).await?;
            parse_identity_bytes(&bytes)
        }
        Err(err) => {
            let _ = fs::remove_file(&tmp_path).await;
            Err(err)
        }
    }
}

fn parse_identity_bytes(bytes: &[u8]) -> io::Result<MeshIdentity> {
    let text = std::str::from_utf8(bytes)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?
        .trim();
    let mut key_bytes = [0_u8; SECRET_KEY_BYTES];
    hex::decode_to_slice(text, &mut key_bytes)
        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
    Ok(MeshIdentity {
        secret_key: SecretKey::from_bytes(&key_bytes),
    })
}

fn temp_path(path: &Path) -> PathBuf {
    let mut name = path
        .file_name()
        .map(|name| name.to_os_string())
        .unwrap_or_else(|| ".mesh-identity".into());
    name.push(format!(".{}.tmp", Uuid::new_v4()));
    path.with_file_name(name)
}

/// A key file copied around or created by an older release may be group or
/// world readable; anyone who can read it can impersonate this node.
#[cfg(unix)]
async fn restrict_existing_secret_file(path: &Path) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let mode = fs::metadata(path).await?.permissions().mode() & 0o777;
    if mode & 0o077 != 0 {
        tracing::warn!(
            path = %path.display(),
            mode = format!("{mode:o}"),
            "mesh identity key was group/world accessible; restricting it to 0600"
        );
        // Read-only mounts (e.g. a container's `:ro` key bind) cannot be
        // tightened from inside; the key is still usable, so refusing to start
        // would turn a hardening nudge into an outage.
        if let Err(error) = fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)).await
        {
            tracing::warn!(
                path = %path.display(),
                %error,
                "could not restrict mesh identity key permissions; fix them on the host"
            );
        }
    }
    Ok(())
}

#[cfg(not(unix))]
async fn restrict_existing_secret_file(_path: &Path) -> io::Result<()> {
    Ok(())
}

/// Directories this process creates for the key are owner-only; existing
/// ancestors are left alone.
#[cfg(unix)]
async fn create_private_dir_all(path: &Path) -> io::Result<()> {
    let mut builder = fs::DirBuilder::new();
    builder.recursive(true).mode(0o700);
    builder.create(path).await
}

#[cfg(not(unix))]
async fn create_private_dir_all(path: &Path) -> io::Result<()> {
    fs::create_dir_all(path).await
}

#[cfg(unix)]
async fn write_new_secret_file(path: &Path, bytes: &[u8]) -> io::Result<()> {
    use tokio::io::AsyncWriteExt;

    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .await?;
    file.write_all(bytes).await?;
    file.write_all(b"\n").await?;
    file.sync_all().await?;
    Ok(())
}

#[cfg(not(unix))]
async fn write_new_secret_file(path: &Path, bytes: &[u8]) -> io::Result<()> {
    use tokio::io::AsyncWriteExt;

    let mut file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .await?;
    file.write_all(bytes).await?;
    file.write_all(b"\n").await?;
    file.sync_all().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_file(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!("llmconduit-{name}-{}", Uuid::new_v4()))
    }

    #[tokio::test]
    async fn identity_persists_across_loads() {
        let path = temp_file("identity");
        let first = load_or_create(&path).await.expect("create identity");
        let second = load_or_create(&path).await.expect("load identity");

        assert_eq!(first.endpoint_id(), second.endpoint_id());
        let _ = fs::remove_file(path).await;
    }

    #[tokio::test]
    async fn rejects_invalid_identity_file() {
        let path = temp_file("bad-identity");
        fs::write(&path, b"not-a-key").await.expect("write");

        let err = load_or_create(&path).await.expect_err("invalid key");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        let _ = fs::remove_file(path).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn created_identity_is_owner_only_on_unix() {
        use std::os::unix::fs::PermissionsExt;

        let path = temp_file("identity-mode");
        let _ = load_or_create(&path).await.expect("create identity");
        let mode = fs::metadata(&path)
            .await
            .expect("metadata")
            .permissions()
            .mode()
            & 0o777;

        assert_eq!(mode, 0o600);
        let _ = fs::remove_file(path).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn loose_existing_identity_is_restricted_and_new_dirs_are_private() {
        use std::os::unix::fs::PermissionsExt;

        let dir = temp_file("identity-dir");
        let path = dir.join("nested").join("node.key");
        let created = load_or_create(&path).await.expect("create identity");
        let dir_mode = fs::metadata(path.parent().unwrap())
            .await
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(dir_mode, 0o700);

        fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644))
            .await
            .unwrap();
        let loaded = load_or_create(&path).await.expect("load identity");
        assert_eq!(created.endpoint_id(), loaded.endpoint_id());
        let mode = fs::metadata(&path).await.unwrap().permissions().mode() & 0o777;
        assert_eq!(mode, 0o600);
        let _ = fs::remove_dir_all(dir).await;
    }
}
