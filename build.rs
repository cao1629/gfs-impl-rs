fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure().compile_protos(&["proto/gfs.proto", "proto/master_state.proto"], &["proto"])?;
    println!("cargo:rerun-if-changed=proto/gfs.proto");
    println!("cargo:rerun-if-changed=proto/master_state.proto");
    Ok(())
}
