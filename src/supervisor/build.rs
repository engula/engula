fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .extern_path(".engula.v1", "::engula_apis")
        .compile(
            &["engula/supervisor/v1/supervisor.proto"],
            &[".", "../apis"],
        )?;
    Ok(())
}
