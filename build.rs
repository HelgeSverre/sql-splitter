fn main() {
    // On Windows, increase the main thread stack size to 8MB to match Linux/macOS defaults.
    // This is required because:
    // - Windows default is 1MB, Linux/macOS is 8MB
    // - RUST_MIN_STACK only affects spawned threads, not the main thread
    // - Complex SQL parsing (especially convert command) can exceed 1MB stack
    //
    // Note: We use CARGO_CFG_TARGET_OS (not #[cfg]) to detect the target OS,
    // which works correctly for cross-compilation scenarios.
    let target_os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    if target_os == "windows" {
        let target_env = std::env::var("CARGO_CFG_TARGET_ENV").unwrap_or_default();

        if target_env == "msvc" {
            // MSVC toolchain (most common on Windows)
            println!("cargo:rustc-link-arg=/STACK:8388608");
        } else {
            // GNU toolchain (MinGW)
            println!("cargo:rustc-link-arg=-Wl,--stack,8388608");
        }
    }
}
