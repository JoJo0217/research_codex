use super::*;

fn bytes(value: &str) -> Option<FileSnapshotState> {
    #[cfg(unix)]
    let permissions = {
        use std::os::unix::fs::PermissionsExt;
        std::fs::Permissions::from_mode(0o644)
    };
    #[cfg(not(unix))]
    let permissions = std::fs::metadata(".").unwrap().permissions();

    Some(FileSnapshotState {
        bytes: value.as_bytes().to_vec(),
        permissions,
    })
}

#[test]
fn snapshot_restore_round_trips_non_git_file() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("note.txt");
    std::fs::write(&path, b"old").unwrap();

    let snapshot = FileSnapshot {
        path: path.clone(),
        before: snapshot_file(&path).unwrap(),
    };
    std::fs::write(&path, b"new").unwrap();

    restore_file_snapshots(&[snapshot]).unwrap();

    assert_eq!(std::fs::read(&path).unwrap(), b"old");
}

#[test]
fn restore_deletes_file_created_after_snapshot() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("created.txt");

    let snapshot = FileSnapshot {
        path: path.clone(),
        before: snapshot_file(&path).unwrap(),
    };
    std::fs::write(&path, b"created").unwrap();

    restore_file_snapshots(&[snapshot]).unwrap();

    assert!(!path.exists());
}

#[cfg(unix)]
#[test]
fn restore_preserves_original_file_permissions() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempdir().unwrap();
    let path = dir.path().join("script.sh");
    std::fs::write(&path, b"#!/bin/sh\n").unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();

    let snapshot = FileSnapshot {
        path: path.clone(),
        before: snapshot_file(&path).unwrap(),
    };
    std::fs::remove_file(&path).unwrap();

    restore_file_snapshots(&[snapshot]).unwrap();

    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o755
    );
}

#[cfg(unix)]
#[test]
fn restore_refuses_to_write_through_symlink() {
    use std::os::unix::fs::symlink;

    let dir = tempdir().unwrap();
    let target_path = dir.path().join("target.txt");
    let symlink_path = dir.path().join("tracked.txt");
    std::fs::write(&target_path, b"target").unwrap();
    symlink(&target_path, &symlink_path).unwrap();

    let snapshot = FileSnapshot {
        path: symlink_path,
        before: bytes("old"),
    };

    assert!(restore_file_snapshots(&[snapshot]).is_err());
    assert_eq!(std::fs::read(&target_path).unwrap(), b"target");
}

#[cfg(unix)]
#[test]
fn restore_refuses_symlinked_parent_on_write_and_delete() {
    use std::os::unix::fs::symlink;

    let dir = tempdir().unwrap();
    let external_dir = dir.path().join("external");
    let linked_parent = dir.path().join("linked");
    let target_path = external_dir.join("tracked.txt");
    std::fs::create_dir(&external_dir).unwrap();
    std::fs::write(&target_path, b"target").unwrap();
    symlink(&external_dir, &linked_parent).unwrap();

    let write_snapshot = FileSnapshot {
        path: linked_parent.join("tracked.txt"),
        before: bytes("old"),
    };
    let delete_snapshot = FileSnapshot {
        path: linked_parent.join("tracked.txt"),
        before: None,
    };

    assert!(snapshot_file(&linked_parent.join("tracked.txt")).is_err());
    assert!(restore_file_snapshots(&[write_snapshot]).is_err());
    assert_eq!(std::fs::read(&target_path).unwrap(), b"target");
    assert!(restore_file_snapshots(&[delete_snapshot]).is_err());
    assert_eq!(std::fs::read(&target_path).unwrap(), b"target");
}

fn apply_successful_patch_snapshot(
    chat: &mut ChatWidget,
    call_id: &str,
    path: &std::path::Path,
    after: &[u8],
) {
    let changes = file_update_changes(path);
    start_file_change(chat, call_id, changes.clone(), /*from_replay*/ false);
    std::fs::write(path, after).unwrap();
    complete_file_change(
        chat,
        call_id,
        changes,
        AppServerPatchApplyStatus::Completed,
        /*from_replay*/ false,
    );
}

fn file_update_changes(path: &std::path::Path) -> Vec<FileUpdateChange> {
    vec![FileUpdateChange {
        path: path.to_string_lossy().to_string(),
        kind: PatchChangeKind::Update { move_path: None },
        diff: String::new(),
    }]
}

fn replay_kind(from_replay: bool) -> Option<ReplayKind> {
    from_replay.then_some(ReplayKind::ThreadSnapshot)
}

fn start_file_change(
    chat: &mut ChatWidget,
    call_id: &str,
    changes: Vec<FileUpdateChange>,
    from_replay: bool,
) {
    chat.handle_server_notification(
        ServerNotification::ItemStarted(ItemStartedNotification {
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
            started_at_ms: 0,
            item: AppServerThreadItem::FileChange {
                id: call_id.to_string(),
                changes,
                status: AppServerPatchApplyStatus::InProgress,
            },
        }),
        replay_kind(from_replay),
    );
}

fn complete_file_change(
    chat: &mut ChatWidget,
    call_id: &str,
    changes: Vec<FileUpdateChange>,
    status: AppServerPatchApplyStatus,
    from_replay: bool,
) {
    chat.handle_server_notification(
        ServerNotification::ItemCompleted(ItemCompletedNotification {
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
            completed_at_ms: 0,
            item: AppServerThreadItem::FileChange {
                id: call_id.to_string(),
                changes,
                status,
            },
        }),
        replay_kind(from_replay),
    );
}

#[tokio::test]
async fn compaction_reset_rebases_file_restore_turn_numbers() {
    let (mut chat, _app_rx, _op_rx) = make_chatwidget_manual(None).await;
    let dir = tempdir().unwrap();
    let first_path = dir.path().join("first.txt");
    let second_path = dir.path().join("second.txt");
    std::fs::write(&first_path, b"first-old").unwrap();
    std::fs::write(&second_path, b"second-old").unwrap();

    chat.visible_user_turn_count = 10;
    chat.reset_backtrack_file_restore_tracking();

    chat.visible_user_turn_count = 11;
    apply_successful_patch_snapshot(&mut chat, "patch-1", &first_path, b"first-new");
    chat.visible_user_turn_count = 12;
    apply_successful_patch_snapshot(&mut chat, "patch-2", &second_path, b"second-new");

    chat.restore_files_for_backtrack(1).unwrap();

    assert_eq!(std::fs::read(&first_path).unwrap(), b"first-new");
    assert_eq!(std::fs::read(&second_path).unwrap(), b"second-old");
}

#[tokio::test]
async fn post_compaction_rollback_rebases_future_file_restore_turns() {
    let (mut chat, _app_rx, _op_rx) = make_chatwidget_manual(None).await;
    let dir = tempdir().unwrap();
    let first_path = dir.path().join("first.txt");
    let second_path = dir.path().join("second.txt");
    std::fs::write(&first_path, b"first-old").unwrap();
    std::fs::write(&second_path, b"second-old").unwrap();

    chat.visible_user_turn_count = 10;
    chat.reset_backtrack_file_restore_tracking();

    chat.visible_user_turn_count = 11;
    apply_successful_patch_snapshot(&mut chat, "patch-1", &first_path, b"first-new");
    chat.truncate_agent_copy_history_to_user_turn_count(0);
    chat.truncate_backtrack_file_snapshots_to_user_turn_count(0);

    chat.visible_user_turn_count = 1;
    apply_successful_patch_snapshot(&mut chat, "patch-2", &second_path, b"second-new");

    assert!(chat.has_backtrack_file_snapshots_after(0));
    chat.restore_files_for_backtrack(0).unwrap();
    assert_eq!(std::fs::read(&first_path).unwrap(), b"first-new");
    assert_eq!(std::fs::read(&second_path).unwrap(), b"second-old");
}

#[tokio::test]
async fn restore_uses_earliest_snapshot_when_file_changed_in_multiple_turns() {
    let (mut chat, _app_rx, _op_rx) = make_chatwidget_manual(None).await;
    let dir = tempdir().unwrap();
    let path = dir.path().join("note.txt");
    std::fs::write(&path, b"new").unwrap();

    chat.backtrack_file_snapshots
        .push(BacktrackFileTurnSnapshot {
            user_turn_count: 1,
            snapshots: vec![FileSnapshot {
                path: path.clone(),
                before: bytes("old"),
            }],
        });
    chat.backtrack_file_snapshots
        .push(BacktrackFileTurnSnapshot {
            user_turn_count: 2,
            snapshots: vec![FileSnapshot {
                path: path.clone(),
                before: bytes("middle"),
            }],
        });

    chat.restore_files_for_backtrack(0).unwrap();

    assert_eq!(std::fs::read(&path).unwrap(), b"old");
}

#[tokio::test]
async fn replayed_patch_events_do_not_create_file_restore_snapshots() {
    let (mut chat, _app_rx, _op_rx) = make_chatwidget_manual(None).await;
    let dir = tempdir().unwrap();
    let path = dir.path().join("note.txt");
    std::fs::write(&path, b"already patched").unwrap();
    let changes = file_update_changes(&path);

    chat.visible_user_turn_count = 1;
    start_file_change(&mut chat, "patch-1", changes.clone(), /*from_replay*/ true);
    complete_file_change(
        &mut chat,
        "patch-1",
        changes,
        AppServerPatchApplyStatus::Completed,
        /*from_replay*/ true,
    );

    assert!(chat.pending_patch_snapshots.is_empty());
    assert!(chat.backtrack_file_snapshots.is_empty());
    assert!(!chat.has_backtrack_file_snapshots_after(0));
}

#[tokio::test]
async fn live_app_server_file_change_snapshots_are_restorable() {
    let (mut chat, _app_rx, _op_rx) = make_chatwidget_manual(None).await;
    let dir = tempdir().unwrap();
    let path = dir.path().join("app-server.txt");
    std::fs::write(&path, b"old").unwrap();
    let path_string = path.to_string_lossy().to_string();
    let changes = vec![FileUpdateChange {
        path: path_string,
        kind: PatchChangeKind::Update { move_path: None },
        diff: String::new(),
    }];

    chat.visible_user_turn_count = 1;
    chat.handle_server_notification(
        ServerNotification::ItemStarted(ItemStartedNotification {
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
            started_at_ms: 0,
            item: AppServerThreadItem::FileChange {
                id: "patch-1".to_string(),
                changes: changes.clone(),
                status: AppServerPatchApplyStatus::InProgress,
            },
        }),
        /*replay_kind*/ None,
    );
    std::fs::write(&path, b"new").unwrap();
    chat.handle_server_notification(
        ServerNotification::ItemCompleted(ItemCompletedNotification {
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
            completed_at_ms: 0,
            item: AppServerThreadItem::FileChange {
                id: "patch-1".to_string(),
                changes,
                status: AppServerPatchApplyStatus::Completed,
            },
        }),
        /*replay_kind*/ None,
    );

    assert!(chat.has_backtrack_file_snapshots_after(0));
    chat.restore_files_for_backtrack(0).unwrap();
    assert_eq!(std::fs::read(&path).unwrap(), b"old");
}

#[tokio::test]
async fn app_server_approval_then_file_change_stays_unrestorable() {
    let (mut chat, _app_rx, _op_rx) = make_chatwidget_manual(None).await;
    let dir = tempdir().unwrap();
    let path = dir.path().join("approval-server.txt");
    std::fs::write(&path, b"old").unwrap();
    let path_string = path.to_string_lossy().to_string();
    let changes = vec![FileUpdateChange {
        path: path_string,
        kind: PatchChangeKind::Update { move_path: None },
        diff: String::new(),
    }];
    let later_path = test_path_buf("/tmp/later-after-approval.txt");

    chat.visible_user_turn_count = 1;
    chat.on_apply_patch_approval_request(
        "request-1".to_string(),
        ApplyPatchApprovalRequestEvent {
            call_id: "patch-approval".to_string(),
            turn_id: "turn-1".to_string(),
            changes: HashMap::new(),
            reason: None,
            grant_root: None,
        },
    );
    chat.handle_server_notification(
        ServerNotification::ItemStarted(ItemStartedNotification {
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
            started_at_ms: 0,
            item: AppServerThreadItem::FileChange {
                id: "patch-approval".to_string(),
                changes: changes.clone(),
                status: AppServerPatchApplyStatus::InProgress,
            },
        }),
        /*replay_kind*/ None,
    );
    std::fs::write(&path, b"new").unwrap();
    chat.handle_server_notification(
        ServerNotification::ItemCompleted(ItemCompletedNotification {
            thread_id: "thread-1".to_string(),
            turn_id: "turn-1".to_string(),
            completed_at_ms: 0,
            item: AppServerThreadItem::FileChange {
                id: "patch-approval".to_string(),
                changes,
                status: AppServerPatchApplyStatus::Completed,
            },
        }),
        /*replay_kind*/ None,
    );
    chat.backtrack_file_snapshots
        .push(BacktrackFileTurnSnapshot {
            user_turn_count: 2,
            snapshots: vec![FileSnapshot {
                path: later_path,
                before: bytes("later"),
            }],
        });

    assert!(!chat.has_backtrack_file_snapshots_after(0));
    assert_eq!(std::fs::read(&path).unwrap(), b"new");
}

#[tokio::test]
async fn failed_restore_rolls_back_files_restored_earlier_in_batch() {
    let (mut chat, _app_rx, _op_rx) = make_chatwidget_manual(None).await;
    let dir = tempdir().unwrap();
    let first_path = dir.path().join("first.txt");
    let blocked_parent = dir.path().join("blocked");
    let second_path = blocked_parent.join("second.txt");
    std::fs::write(&first_path, b"current").unwrap();
    std::fs::write(&blocked_parent, b"not a directory").unwrap();

    chat.backtrack_file_snapshots
        .push(BacktrackFileTurnSnapshot {
            user_turn_count: 1,
            snapshots: vec![
                FileSnapshot {
                    path: first_path.clone(),
                    before: bytes("old"),
                },
                FileSnapshot {
                    path: second_path,
                    before: bytes("second"),
                },
            ],
        });

    assert!(chat.restore_files_for_backtrack(0).is_err());
    assert_eq!(std::fs::read(&first_path).unwrap(), b"current");
}

#[tokio::test]
async fn failed_patch_marks_turn_unrestorable_instead_of_restoring_snapshot() {
    let (mut chat, _app_rx, _op_rx) = make_chatwidget_manual(None).await;
    let dir = tempdir().unwrap();
    let path = dir.path().join("partial.txt");
    let later_path = test_path_buf("/tmp/later-after-failed.txt");

    chat.pending_patch_snapshots.insert(
        "patch-1".to_string(),
        PendingPatchSnapshots {
            user_turn_count: 1,
            snapshots: vec![FileSnapshot {
                path: path.clone(),
                before: None,
            }],
            restorable: true,
        },
    );
    std::fs::write(&path, b"partial").unwrap();

    chat.finish_pending_patch_snapshots("patch-1", &AppServerPatchApplyStatus::Failed);

    chat.backtrack_file_snapshots
        .push(BacktrackFileTurnSnapshot {
            user_turn_count: 2,
            snapshots: vec![FileSnapshot {
                path: later_path,
                before: bytes("later"),
            }],
        });

    assert!(!chat.has_backtrack_file_snapshots_after(0));
    assert!(path.exists());
}

#[tokio::test]
async fn failed_patch_drops_snapshot_when_files_unchanged() {
    let (mut chat, _app_rx, _op_rx) = make_chatwidget_manual(None).await;
    let dir = tempdir().unwrap();
    let path = dir.path().join("unchanged.txt");

    chat.pending_patch_snapshots.insert(
        "patch-1".to_string(),
        PendingPatchSnapshots {
            user_turn_count: 1,
            snapshots: vec![FileSnapshot { path, before: None }],
            restorable: true,
        },
    );

    chat.finish_pending_patch_snapshots("patch-1", &AppServerPatchApplyStatus::Failed);

    assert!(!chat.has_backtrack_file_snapshots_after(0));
}

#[tokio::test]
async fn approval_required_failed_patch_does_not_restore_external_changes() {
    let (mut chat, _app_rx, _op_rx) = make_chatwidget_manual(None).await;
    let dir = tempdir().unwrap();
    let path = dir.path().join("approval.txt");
    std::fs::write(&path, b"A").unwrap();

    chat.visible_user_turn_count = 1;
    chat.on_apply_patch_approval_request(
        "request-1".to_string(),
        ApplyPatchApprovalRequestEvent {
            call_id: "patch-approval".to_string(),
            turn_id: "turn-1".to_string(),
            changes: HashMap::new(),
            reason: None,
            grant_root: None,
        },
    );
    std::fs::write(&path, b"B").unwrap();
    chat.finish_pending_patch_snapshots("patch-approval", &AppServerPatchApplyStatus::Failed);

    assert!(!chat.has_backtrack_file_snapshots_after(0));
    assert_eq!(std::fs::read(&path).unwrap(), b"B");
}

#[tokio::test]
async fn failed_patch_with_snapshot_error_marks_turn_unrestorable() {
    let (mut chat, _app_rx, _op_rx) = make_chatwidget_manual(None).await;
    let later_path = test_path_buf("/tmp/later.txt");

    chat.pending_patch_snapshots.insert(
        "patch-1".to_string(),
        PendingPatchSnapshots {
            user_turn_count: 1,
            snapshots: Vec::new(),
            restorable: false,
        },
    );
    chat.finish_pending_patch_snapshots("patch-1", &AppServerPatchApplyStatus::Failed);
    chat.backtrack_file_snapshots
        .push(BacktrackFileTurnSnapshot {
            user_turn_count: 2,
            snapshots: vec![FileSnapshot {
                path: later_path,
                before: bytes("later"),
            }],
        });

    assert!(!chat.has_backtrack_file_snapshots_after(0));
    assert!(chat.has_backtrack_file_snapshots_after(1));
}

#[tokio::test]
async fn unrestorable_turn_suppresses_partial_code_restore_offer() {
    let (mut chat, _app_rx, _op_rx) = make_chatwidget_manual(None).await;
    let first_path = test_path_buf("/tmp/first.txt");
    let second_path = test_path_buf("/tmp/second.txt");

    chat.backtrack_unrestorable_file_turns.insert(1);
    chat.backtrack_file_snapshots
        .push(BacktrackFileTurnSnapshot {
            user_turn_count: 2,
            snapshots: vec![FileSnapshot {
                path: second_path,
                before: bytes("second"),
            }],
        });
    chat.backtrack_file_snapshots
        .push(BacktrackFileTurnSnapshot {
            user_turn_count: 1,
            snapshots: vec![FileSnapshot {
                path: first_path,
                before: bytes("first"),
            }],
        });

    assert!(!chat.has_backtrack_file_snapshots_after(0));
    assert!(chat.has_backtrack_file_snapshots_after(1));
}

#[tokio::test]
async fn restore_rechecks_unrestorable_and_pending_ranges() {
    let (mut chat, _app_rx, _op_rx) = make_chatwidget_manual(None).await;
    let dir = tempdir().unwrap();
    let path = dir.path().join("tracked.txt");
    std::fs::write(&path, b"current").unwrap();

    chat.backtrack_file_snapshots
        .push(BacktrackFileTurnSnapshot {
            user_turn_count: 1,
            snapshots: vec![FileSnapshot {
                path: path.clone(),
                before: bytes("old"),
            }],
        });

    chat.backtrack_unrestorable_file_turns.insert(1);
    assert!(chat.restore_files_for_backtrack(0).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), b"current");

    chat.backtrack_unrestorable_file_turns.clear();
    chat.pending_patch_snapshots.insert(
        "pending".to_string(),
        PendingPatchSnapshots {
            user_turn_count: 1,
            snapshots: Vec::new(),
            restorable: true,
        },
    );
    assert!(chat.restore_files_for_backtrack(0).is_err());
    assert_eq!(std::fs::read(&path).unwrap(), b"current");
}
