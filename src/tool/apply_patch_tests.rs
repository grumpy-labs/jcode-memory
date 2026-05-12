use super::*;
use std::io::Write;
use tempfile::NamedTempFile;

fn write_temp(content: &str) -> NamedTempFile {
    let mut f = NamedTempFile::new().unwrap();
    f.write_all(content.as_bytes()).unwrap();
    f
}

#[test]
fn test_parse_add_file() {
    let patch =
        "*** Begin Patch\n*** Add File: hello.txt\n+Hello world\n+Second line\n*** End Patch";
    let hunks = parse_apply_patch(patch).unwrap();
    assert_eq!(hunks.len(), 1);
    match &hunks[0] {
        PatchHunk::AddFile { path, contents } => {
            assert_eq!(path, "hello.txt");
            assert_eq!(contents, "Hello world\nSecond line\n");
        }
        _ => panic!("Expected AddFile"),
    }
}

#[test]
fn test_parse_delete_file() {
    let patch = "*** Begin Patch\n*** Delete File: old.txt\n*** End Patch";
    let hunks = parse_apply_patch(patch).unwrap();
    assert_eq!(hunks.len(), 1);
    match &hunks[0] {
        PatchHunk::DeleteFile { path } => {
            assert_eq!(path, "old.txt");
        }
        _ => panic!("Expected DeleteFile"),
    }
}

#[test]
fn test_parse_update_file_simple() {
    let patch = "*** Begin Patch\n*** Update File: test.py\n@@\n foo\n-bar\n+baz\n*** End Patch";
    let hunks = parse_apply_patch(patch).unwrap();
    assert_eq!(hunks.len(), 1);
    match &hunks[0] {
        PatchHunk::UpdateFile { path, chunks, .. } => {
            assert_eq!(path, "test.py");
            assert_eq!(chunks.len(), 1);
            assert_eq!(chunks[0].old_lines, vec!["foo", "bar"]);
            assert_eq!(chunks[0].new_lines, vec!["foo", "baz"]);
        }
        _ => panic!("Expected UpdateFile"),
    }
}

#[test]
fn test_parse_update_with_context() {
    let patch = "*** Begin Patch\n*** Update File: test.py\n@@ def my_func():\n-    pass\n+    return 42\n*** End Patch";
    let hunks = parse_apply_patch(patch).unwrap();
    match &hunks[0] {
        PatchHunk::UpdateFile { chunks, .. } => {
            assert_eq!(chunks[0].change_context, Some("def my_func():".to_string()));
            assert_eq!(chunks[0].old_lines, vec!["    pass"]);
            assert_eq!(chunks[0].new_lines, vec!["    return 42"]);
        }
        _ => panic!("Expected UpdateFile"),
    }
}

#[test]
fn test_parse_update_with_move() {
    let patch = "*** Begin Patch\n*** Update File: old.py\n*** Move to: new.py\n@@\n-old_line\n+new_line\n*** End Patch";
    let hunks = parse_apply_patch(patch).unwrap();
    match &hunks[0] {
        PatchHunk::UpdateFile {
            path,
            move_to,
            chunks,
        } => {
            assert_eq!(path, "old.py");
            assert_eq!(move_to, &Some("new.py".to_string()));
            assert_eq!(chunks.len(), 1);
        }
        _ => panic!("Expected UpdateFile"),
    }
}

#[test]
fn test_parse_multiple_chunks() {
    let patch = "*** Begin Patch\n*** Update File: test.py\n@@\n foo\n-bar\n+BAR\n@@\n baz\n-qux\n+QUX\n*** End Patch";
    let hunks = parse_apply_patch(patch).unwrap();
    match &hunks[0] {
        PatchHunk::UpdateFile { chunks, .. } => {
            assert_eq!(chunks.len(), 2);
            assert_eq!(chunks[0].old_lines, vec!["foo", "bar"]);
            assert_eq!(chunks[0].new_lines, vec!["foo", "BAR"]);
            assert_eq!(chunks[1].old_lines, vec!["baz", "qux"]);
            assert_eq!(chunks[1].new_lines, vec!["baz", "QUX"]);
        }
        _ => panic!("Expected UpdateFile"),
    }
}

#[test]
fn test_parse_end_of_file() {
    let patch = "*** Begin Patch\n*** Update File: test.py\n@@\n last_line\n+new_last_line\n*** End of File\n*** End Patch";
    let hunks = parse_apply_patch(patch).unwrap();
    match &hunks[0] {
        PatchHunk::UpdateFile { chunks, .. } => {
            assert!(chunks[0].is_end_of_file);
        }
        _ => panic!("Expected UpdateFile"),
    }
}

#[tokio::test]
async fn test_apply_update_simple() {
    let f = write_temp("foo\nbar\n");
    let chunks = vec![UpdateFileChunk {
        change_context: None,
        old_lines: vec!["foo".to_string(), "bar".to_string()],
        new_lines: vec!["foo".to_string(), "baz".to_string()],
        is_end_of_file: false,
    }];
    let (old_result, new_result) = apply_update_chunks(f.path(), &chunks).await.unwrap();
    assert_eq!(old_result, "foo\nbar\n");
    assert_eq!(new_result, "foo\nbaz\n");
}

#[tokio::test]
async fn ambient_apply_patch_delete_archives_removed_file() {
    let _guard = crate::storage::lock_test_env();
    let temp = tempfile::TempDir::new().expect("tempdir");
    let prev_home = std::env::var_os("JCODE_HOME");
    crate::env::set_var("JCODE_HOME", temp.path().join("home"));
    let work = temp.path().join("work");
    std::fs::create_dir_all(&work).expect("work dir");
    std::fs::write(work.join("old.md"), "delete me, but recoverably\n").expect("seed file");

    crate::tool::ambient::register_ambient_session("ambient_archive_delete".to_string());
    let tool = ApplyPatchTool::new();
    let output = tool
        .execute(
            serde_json::json!({
                "patch_text": "*** Begin Patch\n*** Delete File: old.md\n*** End Patch"
            }),
            crate::tool::ToolContext {
                session_id: "ambient_archive_delete".to_string(),
                message_id: "message_1".to_string(),
                tool_call_id: "call_delete".to_string(),
                working_dir: Some(work.clone()),
                stdin_request_tx: None,
                graceful_shutdown_signal: None,
                execution_mode: crate::tool::ToolExecutionMode::AgentTurn,
            },
        )
        .await
        .expect("ambient delete patch should succeed");
    crate::tool::ambient::unregister_ambient_session("ambient_archive_delete");

    assert!(!work.join("old.md").exists());
    assert!(output.output.contains("Archived previous version at"));
    let manifest = temp.path().join("home/ambient/archive/manifest.jsonl");
    let manifest_text = std::fs::read_to_string(&manifest).expect("archive manifest");
    assert!(manifest_text.contains("old.md"));
    assert!(manifest_text.contains("call_delete"));

    let archived = std::fs::read_dir(temp.path().join("home/ambient/archive"))
        .expect("archive dir")
        .flatten()
        .filter(|entry| entry.path().is_dir())
        .map(|entry| entry.path().join("old.md"))
        .find(|path| path.exists())
        .expect("archived deleted file");
    assert_eq!(
        std::fs::read_to_string(archived).expect("archived content"),
        "delete me, but recoverably\n"
    );

    if let Some(prev_home) = prev_home {
        crate::env::set_var("JCODE_HOME", prev_home);
    } else {
        crate::env::remove_var("JCODE_HOME");
    }
}

#[tokio::test]
async fn ambient_apply_patch_add_archives_existing_file_before_overwrite() {
    let _guard = crate::storage::lock_test_env();
    let temp = tempfile::TempDir::new().expect("tempdir");
    let prev_home = std::env::var_os("JCODE_HOME");
    crate::env::set_var("JCODE_HOME", temp.path().join("home"));
    let work = temp.path().join("work");
    std::fs::create_dir_all(&work).expect("work dir");
    std::fs::write(work.join("existing.md"), "recover this\n").expect("seed file");

    crate::tool::ambient::register_ambient_session("ambient_archive_add".to_string());
    let tool = ApplyPatchTool::new();
    let output = tool
        .execute(
            serde_json::json!({
                "patch_text": "*** Begin Patch\n*** Add File: existing.md\n+replacement\n*** End Patch"
            }),
            crate::tool::ToolContext {
                session_id: "ambient_archive_add".to_string(),
                message_id: "message_1".to_string(),
                tool_call_id: "call_add".to_string(),
                working_dir: Some(work.clone()),
                stdin_request_tx: None,
                graceful_shutdown_signal: None,
                execution_mode: crate::tool::ToolExecutionMode::AgentTurn,
            },
        )
        .await
        .expect("ambient add overwrite patch should succeed");
    crate::tool::ambient::unregister_ambient_session("ambient_archive_add");

    assert_eq!(
        std::fs::read_to_string(work.join("existing.md")).expect("new content"),
        "replacement\n"
    );
    assert!(output.output.contains("Archived previous version at"));
    let manifest = temp.path().join("home/ambient/archive/manifest.jsonl");
    let manifest_text = std::fs::read_to_string(&manifest).expect("archive manifest");
    assert!(manifest_text.contains("apply_patch_add_overwrite"));
    assert!(manifest_text.contains("call_add"));

    let archived = std::fs::read_dir(temp.path().join("home/ambient/archive"))
        .expect("archive dir")
        .flatten()
        .filter(|entry| entry.path().is_dir())
        .map(|entry| entry.path().join("existing.md"))
        .find(|path| path.exists())
        .expect("archived overwritten file");
    assert_eq!(
        std::fs::read_to_string(archived).expect("archived content"),
        "recover this\n"
    );

    if let Some(prev_home) = prev_home {
        crate::env::set_var("JCODE_HOME", prev_home);
    } else {
        crate::env::remove_var("JCODE_HOME");
    }
}

#[tokio::test]
async fn ambient_apply_patch_move_archives_source_and_existing_destination() {
    let _guard = crate::storage::lock_test_env();
    let temp = tempfile::TempDir::new().expect("tempdir");
    let prev_home = std::env::var_os("JCODE_HOME");
    crate::env::set_var("JCODE_HOME", temp.path().join("home"));
    let work = temp.path().join("work");
    std::fs::create_dir_all(&work).expect("work dir");
    std::fs::write(work.join("old.md"), "old line\n").expect("seed source");
    std::fs::write(work.join("dest.md"), "dest previous\n").expect("seed destination");

    crate::tool::ambient::register_ambient_session("ambient_archive_move".to_string());
    let tool = ApplyPatchTool::new();
    let output = tool
        .execute(
            serde_json::json!({
                "patch_text": "*** Begin Patch\n*** Update File: old.md\n*** Move to: dest.md\n@@\n-old line\n+new line\n*** End Patch"
            }),
            crate::tool::ToolContext {
                session_id: "ambient_archive_move".to_string(),
                message_id: "message_1".to_string(),
                tool_call_id: "call_move".to_string(),
                working_dir: Some(work.clone()),
                stdin_request_tx: None,
                graceful_shutdown_signal: None,
                execution_mode: crate::tool::ToolExecutionMode::AgentTurn,
            },
        )
        .await
        .expect("ambient move patch should succeed");
    crate::tool::ambient::unregister_ambient_session("ambient_archive_move");

    assert!(!work.join("old.md").exists());
    assert_eq!(
        std::fs::read_to_string(work.join("dest.md")).expect("moved content"),
        "new line\n"
    );
    assert_eq!(
        output
            .output
            .matches("Archived previous version at")
            .count(),
        2
    );
    let manifest = temp.path().join("home/ambient/archive/manifest.jsonl");
    let manifest_text = std::fs::read_to_string(&manifest).expect("archive manifest");
    assert!(manifest_text.contains("apply_patch_move_source"));
    assert!(manifest_text.contains("apply_patch_move_destination"));
    assert!(manifest_text.contains("call_move"));

    let archive_files = std::fs::read_dir(temp.path().join("home/ambient/archive"))
        .expect("archive dir")
        .flatten()
        .filter(|entry| entry.path().is_dir())
        .flat_map(|entry| [entry.path().join("old.md"), entry.path().join("dest.md")])
        .filter(|path| path.exists())
        .map(|path| std::fs::read_to_string(path).expect("archived content"))
        .collect::<Vec<_>>();
    assert!(archive_files.iter().any(|content| content == "old line\n"));
    assert!(
        archive_files
            .iter()
            .any(|content| content == "dest previous\n")
    );

    if let Some(prev_home) = prev_home {
        crate::env::set_var("JCODE_HOME", prev_home);
    } else {
        crate::env::remove_var("JCODE_HOME");
    }
}

#[tokio::test]
async fn test_apply_update_multiple_chunks() {
    let f = write_temp("foo\nbar\nbaz\nqux\n");
    let chunks = vec![
        UpdateFileChunk {
            change_context: None,
            old_lines: vec!["foo".to_string(), "bar".to_string()],
            new_lines: vec!["foo".to_string(), "BAR".to_string()],
            is_end_of_file: false,
        },
        UpdateFileChunk {
            change_context: None,
            old_lines: vec!["baz".to_string(), "qux".to_string()],
            new_lines: vec!["baz".to_string(), "QUX".to_string()],
            is_end_of_file: false,
        },
    ];
    let (old_result, new_result) = apply_update_chunks(f.path(), &chunks).await.unwrap();
    assert_eq!(old_result, "foo\nbar\nbaz\nqux\n");
    assert_eq!(new_result, "foo\nBAR\nbaz\nQUX\n");
}

#[tokio::test]
async fn test_apply_update_with_context_header() {
    let f = write_temp(
        "class Foo:\n    def bar(self):\n        pass\n    def baz(self):\n        pass\n",
    );
    let chunks = vec![UpdateFileChunk {
        change_context: Some("def baz(self):".to_string()),
        old_lines: vec!["        pass".to_string()],
        new_lines: vec!["        return 42".to_string()],
        is_end_of_file: false,
    }];
    let (_old_result, new_result) = apply_update_chunks(f.path(), &chunks).await.unwrap();
    assert_eq!(
        new_result,
        "class Foo:\n    def bar(self):\n        pass\n    def baz(self):\n        return 42\n"
    );
}

#[tokio::test]
async fn test_apply_update_append_at_eof() {
    let f = write_temp("foo\nbar\nbaz\n");
    let chunks = vec![UpdateFileChunk {
        change_context: None,
        old_lines: vec![],
        new_lines: vec!["quux".to_string()],
        is_end_of_file: false,
    }];
    let (_old_result, new_result) = apply_update_chunks(f.path(), &chunks).await.unwrap();
    assert_eq!(new_result, "foo\nbar\nbaz\nquux\n");
}

#[test]
fn test_generate_diff_summary_compact_format() {
    let old = "line one\nline two\nline three\n";
    let new = "line one\nchanged two\nline three\n";
    let diff = generate_diff_summary(old, new);

    assert!(diff.contains("2- line two"));
    assert!(diff.contains("2+ changed two"));
    assert!(!diff.contains("line one"));
}

#[test]
fn test_seek_sequence_exact() {
    let lines: Vec<String> = vec!["foo", "bar", "baz"]
        .into_iter()
        .map(String::from)
        .collect();
    let pattern: Vec<String> = vec!["bar", "baz"].into_iter().map(String::from).collect();
    assert_eq!(seek_sequence(&lines, &pattern, 0, false), Some(1));
}

#[test]
fn test_seek_sequence_whitespace_tolerant() {
    let lines: Vec<String> = vec!["foo   ", "bar\t"]
        .into_iter()
        .map(String::from)
        .collect();
    let pattern: Vec<String> = vec!["foo", "bar"].into_iter().map(String::from).collect();
    assert_eq!(seek_sequence(&lines, &pattern, 0, false), Some(0));
}

#[test]
fn test_seek_sequence_eof() {
    let lines: Vec<String> = vec!["a", "b", "c", "d"]
        .into_iter()
        .map(String::from)
        .collect();
    let pattern: Vec<String> = vec!["c", "d"].into_iter().map(String::from).collect();
    assert_eq!(seek_sequence(&lines, &pattern, 0, true), Some(2));
}

#[test]
fn test_parse_no_begin() {
    let result = parse_apply_patch("random text");
    assert!(result.is_err());
}

#[test]
fn test_parse_heredoc_wrapper() {
    let patch = "<<'EOF'\n*** Begin Patch\n*** Add File: test.txt\n+hello\n*** End Patch\nEOF";
    let hunks = parse_apply_patch(patch).unwrap();
    assert_eq!(hunks.len(), 1);
}

#[test]
fn test_parse_update_without_explicit_at() {
    let patch = "*** Begin Patch\n*** Update File: file.py\n import foo\n+bar\n*** End Patch";
    let hunks = parse_apply_patch(patch).unwrap();
    match &hunks[0] {
        PatchHunk::UpdateFile { chunks, .. } => {
            assert_eq!(chunks.len(), 1);
            assert!(chunks[0].change_context.is_none());
        }
        _ => panic!("Expected UpdateFile"),
    }
}
