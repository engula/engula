fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure().compile(
        &[
            "src/fs/fs.proto",
            "src/format/format.proto",
            "src/journal/journal.proto",
            "src/manifest/manifest.proto",
            "src/compaction/compaction.proto",
        ],
        &["src"],
    )?;
    Ok(())
}
