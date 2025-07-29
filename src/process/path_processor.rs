#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct ProcessedPath {
    pub original: String,
    pub normalized: String,
    pub variants: Vec<String>,
}

pub struct PathProcessor;

impl PathProcessor {
    pub fn new() -> Self {
        Self
    }

    pub fn process_path(&self, path: &str) -> ProcessedPath {
        let normalized = self.normalize_path(path);
        let variants = self.generate_variants(&normalized);

        ProcessedPath {
            original: path.to_string(),
            normalized,
            variants,
        }
    }

    fn normalize_path(&self, path: &str) -> String {
        path.to_lowercase().replace('\\', "/")
    }

    fn generate_variants(&self, path: &str) -> Vec<String> {
        let mut variants = Vec::new();
        let split_path: Vec<&str> = path.split('/').collect();

        // Generate suffix combinations (like the Node.js implementation)
        for i in 1..split_path.len() {
            let suffix = split_path[split_path.len() - i..].join("/");
            if !suffix.is_empty() {
                variants.push(suffix);
            }
        }

        // Create variants with 64-bit identifiers removed
        let original_variants = variants.clone();
        for variant in original_variants {
            let mut cleaned = variant.clone();

            // Remove various 64-bit patterns (matching Node.js logic)
            cleaned = cleaned.replace("64", "");
            if cleaned != variant {
                variants.push(cleaned.clone());
            }

            cleaned = variant.replace(".x64", "");
            if cleaned != variant {
                variants.push(cleaned.clone());
            }

            cleaned = variant.replace("x64", "");
            if cleaned != variant {
                variants.push(cleaned.clone());
            }

            cleaned = variant.replace("_64", "");
            if cleaned != variant {
                variants.push(cleaned);
            }
        }

        // Remove duplicates while preserving order
        let mut seen = std::collections::HashSet::new();
        variants.retain(|v| seen.insert(v.clone()));

        variants
    }
}

impl Default for PathProcessor {
    fn default() -> Self {
        Self::new()
    }
}
