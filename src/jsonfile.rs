//! JSON state files written for other processes to read: the query log and the census.

use std::path::Path;

use anyhow::{Context, Result};
use serde::Serialize;

/// Writes `value` to `path` through a temporary file in the same directory and a rename, so a
/// reader sees the old file or the new one, never half of one.
pub fn write_atomic<T: Serialize + ?Sized>(path: &Path, value: &T) -> Result<()> {
    let file_name = path
        .file_name()
        .with_context(|| format!("path {} has no file name", path.display()))?;
    let mut tmp_name = std::ffi::OsString::from(".");
    tmp_name.push(file_name);
    tmp_name.push(".tmp");
    let tmp = path.with_file_name(tmp_name);
    let result = std::fs::write(&tmp, serde_json::to_vec(value)?)
        .and_then(|()| std::fs::rename(&tmp, path))
        .with_context(|| format!("write {}", path.display()));
    if result.is_err() {
        let _ = std::fs::remove_file(&tmp);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Doc {
        n: u32,
    }

    fn read(path: &Path) -> Doc {
        serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap()
    }

    #[test]
    fn file_written_atomically() {
        let d = tempfile::tempdir().unwrap();
        let path = d.path().join("state.json");
        std::fs::write(&path, "old").unwrap();
        write_atomic(&path, &Doc { n: 7 }).unwrap();
        // The final path holds a complete document and no temporary file is left behind.
        assert_eq!(read(&path), Doc { n: 7 });
        let files: Vec<_> = std::fs::read_dir(d.path())
            .unwrap()
            .map(|e| e.unwrap().file_name())
            .collect();
        assert_eq!(files, ["state.json"]);

        // A failed rename (the target is a directory) cleans up its temporary file and
        // leaves the target alone.
        let blocked = d.path().join("blocked.json");
        std::fs::create_dir(&blocked).unwrap();
        assert!(write_atomic(&blocked, &Doc { n: 1 }).is_err());
        assert!(blocked.is_dir());
        assert_eq!(std::fs::read_dir(d.path()).unwrap().count(), 2);
    }

    #[test]
    fn a_write_that_cannot_create_its_temporary_file_leaves_the_old_file() {
        use std::os::unix::fs::PermissionsExt;

        let d = tempfile::tempdir().unwrap();
        let path = d.path().join("state.json");
        std::fs::write(&path, "old").unwrap();
        std::fs::set_permissions(d.path(), std::fs::Permissions::from_mode(0o555)).unwrap();
        // Root ignores directory permissions, so the scenario cannot be built.
        let probe = d.path().join("probe");
        let is_root = std::fs::write(&probe, "").is_ok();
        if !is_root {
            let result = write_atomic(&path, &Doc { n: 7 });
            let after = std::fs::read_to_string(&path);
            std::fs::set_permissions(d.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
            assert!(result.is_err(), "an in-place write would have succeeded");
            assert_eq!(after.unwrap(), "old");
        }
        std::fs::set_permissions(d.path(), std::fs::Permissions::from_mode(0o755)).unwrap();
    }
}
