use serial_test::serial;
use std::fs;
use std::path::PathBuf;
use std::process::Command;

fn fixture(path: &str) -> PathBuf {
    fs::canonicalize(PathBuf::from(path)).expect("fixture path")
}

fn isolated_home() -> tempfile::TempDir {
    let home = tempfile::TempDir::new().unwrap();
    fs::write(home.path().join(".bashrc"), "").unwrap();
    fs::write(home.path().join(".zshrc"), "").unwrap();
    home
}

#[test]
#[serial]
#[cfg_attr(not(target_os = "linux"), ignore)]
fn install_sh_succeeds_with_valid_checksum() {
    // Clean up any stale lock from previous runs (CI race condition mitigation)
    let _ = std::fs::remove_dir_all("/tmp/coding-agent-search-install.lock.d");
    let tar = fixture("tests/fixtures/install/coding-agent-search-vtest-linux-x86_64.tar.gz");
    let checksum = fs::read_to_string(
        "tests/fixtures/install/coding-agent-search-vtest-linux-x86_64.tar.gz.sha256",
    )
    .unwrap()
    .trim()
    .to_string();
    let dest = tempfile::TempDir::new().unwrap();
    let home = isolated_home();

    let status = Command::new("bash")
        .arg("install.sh")
        .arg("--version")
        .arg("vtest")
        .arg("--dest")
        .arg(dest.path())
        .arg("--easy-mode")
        .env("HOME", home.path())
        .env("ARTIFACT_URL", format!("file://{}", tar.display()))
        .env("CHECKSUM", checksum)
        .status()
        .expect("run install.sh");

    assert!(status.success());
    let bin = dest.path().join("cass");
    assert!(bin.exists());
    let output = Command::new(&bin).output().expect("run installed bin");
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("fixture-linux"));
}

#[test]
#[serial]
#[cfg_attr(not(target_os = "linux"), ignore)]
fn install_sh_fails_with_bad_checksum() {
    // Clean up any stale lock from previous runs (CI race condition mitigation)
    let _ = std::fs::remove_dir_all("/tmp/coding-agent-search-install.lock.d");
    let tar = fixture("tests/fixtures/install/coding-agent-search-vtest-linux-x86_64.tar.gz");
    let dest = tempfile::TempDir::new().unwrap();
    let home = isolated_home();

    let status = Command::new("bash")
        .arg("install.sh")
        .arg("--version")
        .arg("vtest")
        .arg("--dest")
        .arg(dest.path())
        .arg("--easy-mode")
        .env("HOME", home.path())
        .env("ARTIFACT_URL", format!("file://{}", tar.display()))
        .env("CHECKSUM", "deadbeef")
        .status()
        .expect("run install.sh");

    assert!(
        !status.success(),
        "install.sh should fail when checksum does not match"
    );
    assert!(
        !dest.path().join("cass").exists(),
        "cass binary should not be installed on checksum failure"
    );
}

fn find_powershell() -> Option<String> {
    for candidate in [&"pwsh", &"powershell"] {
        if let Ok(path) = which::which(candidate) {
            return Some(path.to_string_lossy().into_owned());
        }
    }
    None
}

#[test]
fn install_ps1_succeeds_with_valid_checksum() {
    if !cfg!(target_os = "windows") {
        eprintln!("skipping powershell test: non-windows runner");
        return;
    }
    let Some(ps) = find_powershell() else {
        eprintln!("skipping powershell test: pwsh not found");
        return;
    };

    let zip = fixture("tests/fixtures/install/coding-agent-search-vtest-windows-x86_64.zip");
    let checksum = fs::read_to_string(
        "tests/fixtures/install/coding-agent-search-vtest-windows-x86_64.zip.sha256",
    )
    .unwrap()
    .trim()
    .to_string();
    let dest = tempfile::TempDir::new().unwrap();

    let status = Command::new(ps)
        .arg("-NoProfile")
        .arg("-ExecutionPolicy")
        .arg("Bypass")
        .arg("-File")
        .arg("install.ps1")
        .arg("-Version")
        .arg("vtest")
        .arg("-Dest")
        .arg(dest.path())
        .arg("-Checksum")
        .arg(&checksum)
        .arg("-EasyMode")
        .arg("-ArtifactUrl")
        .arg(format!("file://{}", zip.display()))
        .status()
        .expect("run install.ps1");

    assert!(status.success());
    let bin = dest.path().join("cass.exe");
    assert!(bin.exists());
    let content = fs::read_to_string(&bin).unwrap();
    assert!(content.contains("fixture-windows"));
}

#[test]
fn install_ps1_fails_with_bad_checksum() {
    if !cfg!(target_os = "windows") {
        eprintln!("skipping powershell test: non-windows runner");
        return;
    }
    let Some(ps) = find_powershell() else {
        eprintln!("skipping powershell test: pwsh not found");
        return;
    };

    let zip = fixture("tests/fixtures/install/coding-agent-search-vtest-windows-x86_64.zip");
    let dest = tempfile::TempDir::new().unwrap();

    let status = Command::new(ps)
        .arg("-NoProfile")
        .arg("-ExecutionPolicy")
        .arg("Bypass")
        .arg("-File")
        .arg("install.ps1")
        .arg("-Version")
        .arg("vtest")
        .arg("-Dest")
        .arg(dest.path())
        .arg("-Checksum")
        .arg("deadbeef")
        .arg("-EasyMode")
        .arg("-ArtifactUrl")
        .arg(format!("file://{}", zip.display()))
        .status()
        .expect("run install.ps1");

    assert!(
        !status.success(),
        "install.ps1 should fail when checksum does not match"
    );
    assert!(
        !dest.path().join("cass.exe").exists(),
        "cass.exe should not be installed on checksum failure"
    );
}

// =============================================================================
// Upgrade Process E2E Tests
// =============================================================================

/// Test that upgrading from an older version to a newer version works correctly.
/// This simulates the full upgrade flow:
/// 1. Install an "old" version
/// 2. Upgrade to a "new" version
/// 3. Verify the new version is correctly installed
#[test]
#[serial]
#[cfg_attr(not(target_os = "linux"), ignore)]
fn upgrade_replaces_existing_binary() {
    // Clean up any stale lock from previous runs
    let _ = std::fs::remove_dir_all("/tmp/coding-agent-search-install.lock.d");

    let tar = fixture("tests/fixtures/install/coding-agent-search-vtest-linux-x86_64.tar.gz");
    let checksum = fs::read_to_string(
        "tests/fixtures/install/coding-agent-search-vtest-linux-x86_64.tar.gz.sha256",
    )
    .unwrap()
    .trim()
    .to_string();
    let dest = tempfile::TempDir::new().unwrap();
    let home = isolated_home();

    // Step 1: Create a test "old" binary to simulate an existing installation
    let bin_path = dest.path().join("cass");
    fs::write(&bin_path, "#!/bin/sh\necho 'old-version-0.0.1'\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(&bin_path).unwrap().permissions();
        perms.set_mode(0o755);
        fs::set_permissions(&bin_path, perms).unwrap();
    }

    // Verify "old" version exists
    let old_output = Command::new(&bin_path).output().expect("run old binary");
    let old_stdout = String::from_utf8_lossy(&old_output.stdout);
    assert!(
        old_stdout.contains("old-version"),
        "old binary should report old version"
    );

    // Step 2: Run the installer to "upgrade"
    let status = Command::new("bash")
        .arg("install.sh")
        .arg("--version")
        .arg("vtest")
        .arg("--dest")
        .arg(dest.path())
        .arg("--easy-mode")
        .env("HOME", home.path())
        .env("ARTIFACT_URL", format!("file://{}", tar.display()))
        .env("CHECKSUM", checksum)
        .status()
        .expect("run install.sh for upgrade");

    assert!(status.success(), "upgrade should succeed");

    // Step 3: Verify the new version replaced the old one
    assert!(bin_path.exists(), "binary should still exist after upgrade");

    let new_output = Command::new(&bin_path)
        .output()
        .expect("run upgraded binary");
    let new_stdout = String::from_utf8_lossy(&new_output.stdout);
    assert!(
        new_stdout.contains("fixture-linux"),
        "upgraded binary should report new version, got: {}",
        new_stdout
    );
    assert!(
        !new_stdout.contains("old-version"),
        "upgraded binary should not report old version"
    );
}

/// Test that the installer correctly handles concurrent upgrade attempts.
/// The lock mechanism should prevent race conditions.
#[test]
#[serial]
#[cfg_attr(not(target_os = "linux"), ignore)]
fn concurrent_installs_are_serialized() {
    // Clean up any stale lock
    let _ = std::fs::remove_dir_all("/tmp/coding-agent-search-install.lock.d");

    let tar = fixture("tests/fixtures/install/coding-agent-search-vtest-linux-x86_64.tar.gz");
    let checksum = fs::read_to_string(
        "tests/fixtures/install/coding-agent-search-vtest-linux-x86_64.tar.gz.sha256",
    )
    .unwrap()
    .trim()
    .to_string();
    let dest1 = tempfile::TempDir::new().unwrap();
    let dest2 = tempfile::TempDir::new().unwrap();
    let home1 = isolated_home();
    let home2 = isolated_home();

    // Spawn two concurrent installs
    let tar1 = tar.clone();
    let checksum1 = checksum.clone();
    let dest1_path = dest1.path().to_path_buf();
    let home1_path = home1.path().to_path_buf();

    let handle1 = std::thread::spawn(move || {
        Command::new("bash")
            .arg("install.sh")
            .arg("--version")
            .arg("vtest")
            .arg("--dest")
            .arg(&dest1_path)
            .arg("--easy-mode")
            .env("HOME", home1_path)
            .env("ARTIFACT_URL", format!("file://{}", tar1.display()))
            .env("CHECKSUM", checksum1)
            .status()
    });

    // Small delay to increase chance of overlap
    std::thread::sleep(std::time::Duration::from_millis(50));

    let tar2 = tar;
    let checksum2 = checksum;
    let dest2_path = dest2.path().to_path_buf();
    let home2_path = home2.path().to_path_buf();

    let handle2 = std::thread::spawn(move || {
        Command::new("bash")
            .arg("install.sh")
            .arg("--version")
            .arg("vtest")
            .arg("--dest")
            .arg(&dest2_path)
            .arg("--easy-mode")
            .env("HOME", home2_path)
            .env("ARTIFACT_URL", format!("file://{}", tar2.display()))
            .env("CHECKSUM", checksum2)
            .status()
    });

    let result1 = handle1.join().expect("thread 1 should complete");
    let result2 = handle2.join().expect("thread 2 should complete");

    let success1 = result1.as_ref().map(|s| s.success()).unwrap_or(false);
    let success2 = result2.as_ref().map(|s| s.success()).unwrap_or(false);

    // One should succeed, one might fail due to lock (or both succeed if serialized)
    // The key is no crashes or corrupted installs
    let success_count = if success1 { 1 } else { 0 } + if success2 { 1 } else { 0 };

    assert!(
        success_count >= 1,
        "at least one concurrent install should succeed"
    );

    // If first succeeded, verify the binary works
    if success1 {
        let bin = dest1.path().join("cass");
        assert!(bin.exists(), "binary should exist after successful install");
    }
}

/// Test that the verify flag actually runs the installed binary.
#[test]
#[serial]
#[cfg_attr(not(target_os = "linux"), ignore)]
fn verify_flag_runs_self_test() {
    // Clean up any stale lock
    let _ = std::fs::remove_dir_all("/tmp/coding-agent-search-install.lock.d");

    let tar = fixture("tests/fixtures/install/coding-agent-search-vtest-linux-x86_64.tar.gz");
    let checksum = fs::read_to_string(
        "tests/fixtures/install/coding-agent-search-vtest-linux-x86_64.tar.gz.sha256",
    )
    .unwrap()
    .trim()
    .to_string();
    let dest = tempfile::TempDir::new().unwrap();
    let home = isolated_home();

    let output = Command::new("bash")
        .arg("install.sh")
        .arg("--version")
        .arg("vtest")
        .arg("--dest")
        .arg(dest.path())
        .arg("--easy-mode")
        .arg("--verify") // This should run the binary after install
        .env("HOME", home.path())
        .env("ARTIFACT_URL", format!("file://{}", tar.display()))
        .env("CHECKSUM", checksum)
        .output()
        .expect("run install.sh with verify");

    assert!(
        output.status.success(),
        "install with verify should succeed"
    );

    let stdout = String::from_utf8_lossy(&output.stdout);
    // The fixture binary outputs "fixture-linux" which should appear in verify output
    assert!(
        stdout.contains("fixture-linux") || stdout.contains("Self-test complete"),
        "verify should run the binary and show output, got: {}",
        stdout
    );
}
