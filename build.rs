//! Compile the gRPC proto into Rust with a pure-Rust toolchain.
//!
//! `protoc` is not assumed to be present in build or CI environments, so the
//! FileDescriptorSet is produced by `protox` (a Rust protobuf compiler) and
//! handed to `tonic-prost-build` for code generation — no external binary.

use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = "proto/blaze/v1/blaze.proto";
    let include = "proto";

    println!("cargo:rerun-if-changed={proto}");
    println!("cargo:rerun-if-changed={include}");

    let descriptors = protox::compile([proto], [include])?;

    let out_dir = PathBuf::from(std::env::var("OUT_DIR")?);
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(true)
        .file_descriptor_set_path(out_dir.join("blaze_descriptor.bin"))
        .compile_fds(descriptors)?;

    Ok(())
}
