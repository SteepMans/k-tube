fn main() {
    // Tubeshka: the gateway address is built in from the environment (CI secret), never from the repository
    println!("cargo:rerun-if-env-changed=TUBESHKA_GATEWAY_URL");
    println!("cargo:rerun-if-env-changed=TUBESHKA_VERSION");
    // application icon (resource id 1 is what the window and taskbar use)
    #[cfg(windows)]
    {
        println!("cargo:rerun-if-changed=assets/streamtube.rc");
        println!("cargo:rerun-if-changed=assets/streamtube.ico");
        println!("cargo:rerun-if-changed=assets/accent");
        let _ = embed_resource::compile("assets/streamtube.rc", embed_resource::NONE);
    }
}
