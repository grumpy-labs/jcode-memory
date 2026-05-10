pub use jcode_build_support::*;

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    #[derive(Deserialize)]
    struct CargoManifest {
        #[serde(default)]
        bin: Vec<CargoBinTarget>,
    }

    #[derive(Deserialize)]
    struct CargoBinTarget {
        name: String,
        #[serde(default, rename = "required-features")]
        required_features: Vec<String>,
    }

    #[test]
    fn auxiliary_bins_require_opt_in_features() {
        let manifest_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml");
        let manifest: CargoManifest =
            toml::from_str(&std::fs::read_to_string(manifest_path).expect("read root Cargo.toml"))
                .expect("parse root Cargo.toml");

        for bin in manifest.bin {
            if matches!(bin.name.as_str(), "jcode" | "jcode-harness") {
                continue;
            }

            assert!(
                bin.required_features
                    .iter()
                    .any(|feature| feature == "dev-bins"),
                "auxiliary bin `{}` should require the dev-bins feature",
                bin.name
            );
        }
    }
}
