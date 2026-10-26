//! Compiles every `presets/*.toml` into the binary as a built-in preset named after its file.

use std::{env, fs, path::PathBuf};

fn main() {
    let dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap()).join("presets");
    // A directory path makes Cargo rerun this script when a file in it is added, removed or edited.
    println!("cargo:rerun-if-changed={}", dir.display());

    let mut presets: Vec<(String, PathBuf)> = fs::read_dir(&dir)
        .expect("read presets/")
        .map(|entry| entry.expect("read presets/ entry").path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "toml"))
        .map(|path| {
            let id = path
                .file_stem()
                .and_then(|s| s.to_str())
                .expect("UTF-8 preset file name");
            (id.to_string(), path.clone())
        })
        .collect();
    presets.sort();

    let mut table = String::from("&[\n");
    for (id, path) in &presets {
        table += &format!(
            "    ({id:?}, include_str!({:?})),\n",
            path.display().to_string()
        );
    }
    table += "]\n";
    fs::write(
        PathBuf::from(env::var("OUT_DIR").unwrap()).join("presets.rs"),
        table,
    )
    .expect("write presets.rs");
}
