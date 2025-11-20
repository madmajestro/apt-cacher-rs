use anyhow::Context as _;
use log::debug;

use crate::global_config;

fn remove_dir_contents<P: AsRef<std::path::Path>>(path: P) -> anyhow::Result<()> {
    for entry in std::fs::read_dir(path)? {
        let entry_path = entry?.path();
        debug!("Removing file `{}`", entry_path.display());
        std::fs::remove_file(&entry_path)
            .with_context(|| format!("Failed to remove entry `{}`", entry_path.display()))?;
    }
    Ok(())
}

pub(crate) fn task_setup() -> anyhow::Result<()> {
    let cache_path = &global_config().cache_directory;

    std::fs::create_dir_all(cache_path)
        .with_context(|| format!("Failed to create directory `{}`", cache_path.display()))?;

    /* Check for modification timestamp support */
    let mdata = std::fs::metadata(cache_path)
        .with_context(|| format!("Failed to inspect directory `{}`", cache_path.display()))?;
    mdata
        .modified()
        .context("No modification timestamp support")?;

    let cache_tmp_path: std::path::PathBuf =
        [cache_path, std::path::Path::new("tmp")].iter().collect();

    std::fs::create_dir_all(&cache_tmp_path)
        .with_context(|| format!("Failed to create directory `{}`", cache_tmp_path.display()))?;

    remove_dir_contents(&cache_tmp_path).with_context(|| {
        format!(
            "Failed to empty out temporary directory `{}`",
            cache_tmp_path.display()
        )
    })?;

    Ok(())
}
