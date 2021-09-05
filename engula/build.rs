fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure().compile(
        &[
            "src/format/format.proto",
            "src/journal/journal.proto",
            "src/manifest/manifest.proto",
            "src/file_system/file_system.proto",
            "src/job/job.proto",
        ],
        &["src"],
    )?;
    Ok(())
}
