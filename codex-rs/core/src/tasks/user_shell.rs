use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use codex_async_utils::CancelErr;
use codex_async_utils::OrCancelExt;
use codex_network_proxy::PROXY_ACTIVE_ENV_KEY;
use codex_network_proxy::PROXY_ENV_KEYS;
#[cfg(target_os = "macos")]
use codex_network_proxy::PROXY_GIT_SSH_COMMAND_ENV_KEY;
use codex_protocol::user_input::UserInput;
use tokio_util::sync::CancellationToken;
use tracing::error;
use uuid::Uuid;

use crate::exec::ExecCapturePolicy;
use crate::exec::StdoutStream;
use crate::exec::execute_exec_request;
use crate::exec_env::create_env;
use crate::sandboxing::ExecRequest;
use crate::shell::ShellType;
use crate::session::turn_context::TurnContext;
use crate::state::TaskKind;
use crate::tools::format_exec_output_str;
use crate::tools::runtimes::maybe_wrap_shell_lc_with_snapshot;
use crate::user_shell_command::user_shell_command_record_item;
use codex_protocol::exec_output::ExecToolCallOutput;
use codex_protocol::exec_output::StreamOutput;
use codex_protocol::protocol::EventMsg;
use codex_protocol::protocol::ExecCommandBeginEvent;
use codex_protocol::protocol::ExecCommandEndEvent;
use codex_protocol::protocol::ExecCommandSource;
use codex_protocol::protocol::ExecCommandStatus;
use codex_protocol::protocol::RolloutItem;
use codex_protocol::protocol::TurnStartedEvent;
use codex_sandboxing::SandboxType;
use codex_shell_command::parse_command::parse_command;
use codex_utils_absolute_path::AbsolutePathBuf;

use super::SessionTask;
use super::SessionTaskContext;
use crate::session::session::Session;
use crate::session::session::SessionSettingsUpdate;
use codex_protocol::models::PermissionProfile;
use codex_protocol::models::ResponseInputItem;
use codex_protocol::models::ResponseItem;

const USER_SHELL_TIMEOUT_MS: u64 = 60 * 60 * 1000; // 1 hour

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum UserShellCommandMode {
    /// Executes as an independent turn lifecycle (emits TurnStarted/TurnComplete
    /// via task lifecycle plumbing).
    StandaloneTurn,
    /// Executes while another turn is already active. This mode must not emit a
    /// second TurnStarted/TurnComplete pair for the same active turn.
    ActiveTurnAuxiliary,
}

#[derive(Clone)]
pub(crate) struct UserShellCommandTask {
    command: String,
}

impl UserShellCommandTask {
    pub(crate) fn new(command: String) -> Self {
        Self { command }
    }
}

impl SessionTask for UserShellCommandTask {
    fn kind(&self) -> TaskKind {
        TaskKind::Regular
    }

    fn span_name(&self) -> &'static str {
        "session_task.user_shell"
    }

    async fn run(
        self: Arc<Self>,
        session: Arc<SessionTaskContext>,
        turn_context: Arc<TurnContext>,
        _input: Vec<UserInput>,
        cancellation_token: CancellationToken,
    ) -> Option<String> {
        execute_user_shell_command(
            session.clone_session(),
            turn_context,
            self.command.clone(),
            cancellation_token,
            UserShellCommandMode::StandaloneTurn,
        )
        .await;
        None
    }
}

pub(crate) async fn execute_user_shell_command(
    session: Arc<Session>,
    turn_context: Arc<TurnContext>,
    command: String,
    cancellation_token: CancellationToken,
    mode: UserShellCommandMode,
) {
    session
        .services
        .session_telemetry
        .counter("codex.task.user_shell", /*inc*/ 1, &[]);

    if mode == UserShellCommandMode::StandaloneTurn {
        // Auxiliary mode runs within an existing active turn. That turn already
        // emitted TurnStarted, so emitting another TurnStarted here would create
        // duplicate turn lifecycle events and confuse clients.
        // TODO(ccunningham): After TurnStarted, emit model-visible turn context diffs for
        // standalone lifecycle tasks (for example /shell, and review once it emits TurnStarted).
        // `/compact` is an intentional exception because compaction requests should not include
        // freshly reinjected context before the summary/replacement history is applied.
        let event = EventMsg::TurnStarted(TurnStartedEvent {
            turn_id: turn_context.sub_id.clone(),
            started_at: turn_context.turn_timing_state.started_at_unix_secs().await,
            model_context_window: turn_context.model_context_window(),
            collaboration_mode_kind: turn_context.collaboration_mode.mode,
        });
        session.send_event(turn_context.as_ref(), event).await;
    }

    // Execute the user's script under their default shell when known; this
    // allows commands that use shell features (pipes, &&, redirects, etc.).
    // We do not source rc files or otherwise reformat the script.
    let use_login_shell = true;
    let session_shell = session.user_shell();
    let display_command = session_shell.derive_exec_args(&command, use_login_shell);
    let mut exec_env_map = create_env(
        &turn_context.shell_environment_policy,
        Some(session.conversation_id),
    );
    if exec_env_map.contains_key(PROXY_ACTIVE_ENV_KEY) {
        for key in PROXY_ENV_KEYS {
            exec_env_map.remove(*key);
        }
        #[cfg(target_os = "macos")]
        if exec_env_map
            .get(PROXY_GIT_SSH_COMMAND_ENV_KEY)
            .is_some_and(|value| {
                value.starts_with(codex_network_proxy::CODEX_PROXY_GIT_SSH_COMMAND_MARKER)
            })
        {
            exec_env_map.remove(PROXY_GIT_SSH_COMMAND_ENV_KEY);
        }
    }
    let exec_command = maybe_wrap_shell_lc_with_snapshot(
        &display_command,
        session_shell.as_ref(),
        &turn_context.cwd,
        &turn_context.shell_environment_policy.r#set,
        &exec_env_map,
    );

    let call_id = Uuid::new_v4().to_string();
    let raw_command = command;
    let cwd = turn_context.cwd.clone();
    let shell_home = exec_env_map
        .get("HOME")
        .map(|value| PathBuf::from(value.as_str()));
    let cdpath_may_affect = exec_env_map.get("CDPATH").is_some_and(|value| !value.is_empty())
        || shell_snapshot_exports_cdpath(session_shell.as_ref(), &turn_context.cwd);

    let parsed_cmd = parse_command(&display_command);
    if matches!(
        session_shell.shell_type,
        ShellType::Bash | ShellType::Zsh | ShellType::Sh
    ) && handle_cd_command(
        &session,
        turn_context.as_ref(),
        &raw_command,
        &display_command,
        &parsed_cmd,
        &call_id,
        shell_home.as_deref(),
        cdpath_may_affect,
        mode,
    )
    .await
    {
        return;
    }

    session
        .send_event(
            turn_context.as_ref(),
            EventMsg::ExecCommandBegin(ExecCommandBeginEvent {
                call_id: call_id.clone(),
                process_id: None,
                turn_id: turn_context.sub_id.clone(),
                command: display_command.clone(),
                cwd: cwd.clone(),
                parsed_cmd: parsed_cmd.clone(),
                source: ExecCommandSource::UserShell,
                interaction_input: None,
            }),
        )
        .await;

    let permission_profile = PermissionProfile::Disabled;
    let exec_env = ExecRequest {
        command: exec_command.clone(),
        cwd: cwd.clone(),
        env: exec_env_map,
        exec_server_env_config: None,
        // `/shell` is the explicit full-access escape hatch, so it must not
        // inherit a managed proxy from the surrounding session or turn.
        network: None,
        // TODO(zhao-oai): Now that we have ExecExpiration::Cancellation, we
        // should use that instead of an "arbitrarily large" timeout here.
        expiration: USER_SHELL_TIMEOUT_MS.into(),
        capture_policy: ExecCapturePolicy::ShellTool,
        sandbox: SandboxType::None,
        windows_sandbox_policy_cwd: cwd.clone(),
        windows_sandbox_level: turn_context.windows_sandbox_level,
        windows_sandbox_private_desktop: turn_context
            .config
            .permissions
            .windows_sandbox_private_desktop,
        permission_profile: permission_profile.clone(),
        file_system_sandbox_policy: permission_profile.file_system_sandbox_policy(),
        network_sandbox_policy: permission_profile.network_sandbox_policy(),
        windows_sandbox_filesystem_overrides: None,
        arg0: None,
    };

    let stdout_stream = Some(StdoutStream {
        sub_id: turn_context.sub_id.clone(),
        call_id: call_id.clone(),
        tx_event: session.get_tx_event(),
    });

    let exec_result = execute_exec_request(exec_env, stdout_stream, /*after_spawn*/ None)
        .or_cancel(&cancellation_token)
        .await;

    match exec_result {
        Err(CancelErr::Cancelled) => {
            let aborted_message = "command aborted by user".to_string();
            let exec_output = ExecToolCallOutput {
                exit_code: -1,
                stdout: StreamOutput::new(String::new()),
                stderr: StreamOutput::new(aborted_message.clone()),
                aggregated_output: StreamOutput::new(aborted_message.clone()),
                duration: Duration::ZERO,
                timed_out: false,
            };
            persist_user_shell_output(
                &session,
                turn_context.as_ref(),
                &raw_command,
                &exec_output,
                mode,
            )
            .await;
            session
                .send_event(
                    turn_context.as_ref(),
                    EventMsg::ExecCommandEnd(ExecCommandEndEvent {
                        call_id,
                        process_id: None,
                        turn_id: turn_context.sub_id.clone(),
                        command: display_command.clone(),
                        cwd: cwd.clone(),
                        parsed_cmd: parsed_cmd.clone(),
                        source: ExecCommandSource::UserShell,
                        interaction_input: None,
                        stdout: String::new(),
                        stderr: aborted_message.clone(),
                        aggregated_output: aborted_message.clone(),
                        exit_code: -1,
                        duration: Duration::ZERO,
                        formatted_output: aborted_message,
                        status: ExecCommandStatus::Failed,
                    }),
                )
                .await;
        }
        Ok(Ok(output)) => {
            session
                .send_event(
                    turn_context.as_ref(),
                    EventMsg::ExecCommandEnd(ExecCommandEndEvent {
                        call_id: call_id.clone(),
                        process_id: None,
                        turn_id: turn_context.sub_id.clone(),
                        command: display_command.clone(),
                        cwd: cwd.clone(),
                        parsed_cmd: parsed_cmd.clone(),
                        source: ExecCommandSource::UserShell,
                        interaction_input: None,
                        stdout: output.stdout.text.clone(),
                        stderr: output.stderr.text.clone(),
                        aggregated_output: output.aggregated_output.text.clone(),
                        exit_code: output.exit_code,
                        duration: output.duration,
                        formatted_output: format_exec_output_str(
                            &output,
                            turn_context.truncation_policy,
                        ),
                        status: if output.exit_code == 0 {
                            ExecCommandStatus::Completed
                        } else {
                            ExecCommandStatus::Failed
                        },
                    }),
                )
                .await;

            persist_user_shell_output(&session, turn_context.as_ref(), &raw_command, &output, mode)
                .await;
        }
        Ok(Err(err)) => {
            error!("user shell command failed: {err:?}");
            let message = format!("execution error: {err:?}");
            let exec_output = ExecToolCallOutput {
                exit_code: -1,
                stdout: StreamOutput::new(String::new()),
                stderr: StreamOutput::new(message.clone()),
                aggregated_output: StreamOutput::new(message.clone()),
                duration: Duration::ZERO,
                timed_out: false,
            };
            session
                .send_event(
                    turn_context.as_ref(),
                    EventMsg::ExecCommandEnd(ExecCommandEndEvent {
                        call_id,
                        process_id: None,
                        turn_id: turn_context.sub_id.clone(),
                        command: display_command,
                        cwd,
                        parsed_cmd,
                        source: ExecCommandSource::UserShell,
                        interaction_input: None,
                        stdout: exec_output.stdout.text.clone(),
                        stderr: exec_output.stderr.text.clone(),
                        aggregated_output: exec_output.aggregated_output.text.clone(),
                        exit_code: exec_output.exit_code,
                        duration: exec_output.duration,
                        formatted_output: format_exec_output_str(
                            &exec_output,
                            turn_context.truncation_policy,
                        ),
                        status: ExecCommandStatus::Failed,
                    }),
                )
                .await;
            persist_user_shell_output(
                &session,
                turn_context.as_ref(),
                &raw_command,
                &exec_output,
                mode,
            )
            .await;
        }
    }
}

async fn handle_cd_command(
    session: &Session,
    turn_context: &TurnContext,
    raw_command: &str,
    display_command: &[String],
    parsed_cmd: &[codex_protocol::parse_command::ParsedCommand],
    call_id: &str,
    home: Option<&Path>,
    cdpath_may_affect: bool,
    mode: UserShellCommandMode,
) -> bool {
    let target = match codex_shell_command::cd::parse_with_home(raw_command, home) {
        codex_shell_command::cd::ShellCd::NotCd => return false,
        codex_shell_command::cd::ShellCd::Invalid(message) => {
            send_cd_end(
                session,
                turn_context,
                raw_command,
                display_command,
                parsed_cmd,
                call_id,
                turn_context.cwd.clone(),
                cd_output(-1, &message),
                mode,
            )
            .await;
            return true;
        }
        codex_shell_command::cd::ShellCd::ChangeTo(target) => target,
    };
    if cdpath_may_affect && cdpath_can_affect(&target) {
        return false;
    }

    let next_cwd = if mode == UserShellCommandMode::ActiveTurnAuxiliary {
        Err("cd: cannot change cwd while a turn is in progress".to_string())
    } else {
        codex_shell_command::cd::resolve(target, turn_context.cwd.as_path())
    };
    let (cwd, output) = match next_cwd {
        Ok(path) => match AbsolutePathBuf::try_from(path.clone()) {
            Ok(abs) => match session
                .update_settings(SessionSettingsUpdate {
                    cwd: Some(path),
                    ..Default::default()
                })
                .await
            {
                Ok(()) => {
                    let mut item = turn_context.to_turn_context_item();
                    item.cwd = abs.to_path_buf();
                    session.persist_rollout_items(&[RolloutItem::TurnContext(item)]).await;
                    let output = cd_output(0, &format!("cwd: {}", abs.display()));
                    (abs, output)
                }
                Err(err) => (
                    turn_context.cwd.clone(),
                    cd_output(-1, &format!("cd: failed to update cwd: {err}")),
                ),
            },
            Err(err) => (
                turn_context.cwd.clone(),
                cd_output(-1, &format!("cd: resolved path is not absolute: {err}")),
            ),
        },
        Err(message) => (turn_context.cwd.clone(), cd_output(-1, &message)),
    };
    send_cd_end(
        session,
        turn_context,
        raw_command,
        display_command,
        parsed_cmd,
        call_id,
        cwd,
        output,
        mode,
    )
    .await;
    true
}

fn cd_output(exit_code: i32, message: &str) -> ExecToolCallOutput {
    let (stdout, stderr) = if exit_code == 0 {
        (message.to_string(), String::new())
    } else {
        (String::new(), message.to_string())
    };
    ExecToolCallOutput {
        exit_code,
        stdout: StreamOutput::new(stdout.clone()),
        stderr: StreamOutput::new(stderr.clone()),
        aggregated_output: StreamOutput::new(if exit_code == 0 { stdout } else { stderr }),
        duration: Duration::ZERO,
        timed_out: false,
    }
}

async fn send_cd_end(
    session: &Session,
    turn_context: &TurnContext,
    raw_command: &str,
    display_command: &[String],
    parsed_cmd: &[codex_protocol::parse_command::ParsedCommand],
    call_id: &str,
    cwd: AbsolutePathBuf,
    output: ExecToolCallOutput,
    mode: UserShellCommandMode,
) {
    session
        .send_event(
            turn_context,
            EventMsg::ExecCommandBegin(ExecCommandBeginEvent {
                call_id: call_id.to_string(),
                process_id: None,
                turn_id: turn_context.sub_id.clone(),
                command: display_command.to_vec(),
                cwd: turn_context.cwd.clone(),
                parsed_cmd: parsed_cmd.to_vec(),
                source: ExecCommandSource::UserShell,
                interaction_input: None,
            }),
        )
        .await;
    session
        .send_event(
            turn_context,
            EventMsg::ExecCommandEnd(ExecCommandEndEvent {
                call_id: call_id.to_string(),
                process_id: None,
                turn_id: turn_context.sub_id.clone(),
                command: display_command.to_vec(),
                cwd,
                parsed_cmd: parsed_cmd.to_vec(),
                source: ExecCommandSource::UserShell,
                interaction_input: None,
                stdout: output.stdout.text.clone(),
                stderr: output.stderr.text.clone(),
                aggregated_output: output.aggregated_output.text.clone(),
                exit_code: output.exit_code,
                duration: output.duration,
                formatted_output: format_exec_output_str(
                    &output,
                    turn_context.truncation_policy,
                ),
                status: if output.exit_code == 0 {
                    ExecCommandStatus::Completed
                } else {
                    ExecCommandStatus::Failed
                },
            }),
        )
        .await;
    persist_user_shell_output(session, turn_context, raw_command, &output, mode).await;
}

fn cdpath_can_affect(target: &Path) -> bool {
    if target.is_absolute() {
        return false;
    }
    target
        .components()
        .next()
        .is_some_and(|component| matches!(component, std::path::Component::Normal(_)))
}

fn shell_snapshot_exports_cdpath(shell: &crate::shell::Shell, cwd: &AbsolutePathBuf) -> bool {
    let Some(snapshot) = shell.shell_snapshot() else {
        return false;
    };
    if !snapshot.path.exists()
        || !crate::path_utils::paths_match_after_normalization(snapshot.cwd.as_path(), cwd)
    {
        return false;
    }
    let Ok(contents) = std::fs::read_to_string(snapshot.path.as_path()) else {
        return true;
    };
    contents
        .lines()
        .skip_while(|line| !line.trim_start().starts_with("# exports "))
        .skip(1)
        .any(|line| {
            let line = line.trim_start();
            line.starts_with("export CDPATH=")
                || line.starts_with("declare -x CDPATH=")
                || line.starts_with("typeset -x CDPATH=")
        })
}

async fn persist_user_shell_output(
    session: &Session,
    turn_context: &TurnContext,
    raw_command: &str,
    exec_output: &ExecToolCallOutput,
    mode: UserShellCommandMode,
) {
    let output_item = user_shell_command_record_item(raw_command, exec_output, turn_context);

    if mode == UserShellCommandMode::StandaloneTurn {
        session
            .record_conversation_items(turn_context, std::slice::from_ref(&output_item))
            .await;
        // Standalone shell turns can run before any regular user turn, so
        // explicitly materialize rollout persistence after recording output.
        session.ensure_rollout_materialized().await;
        return;
    }

    let response_input_item = match output_item {
        ResponseItem::Message {
            role,
            content,
            phase,
            ..
        } => ResponseInputItem::Message {
            role,
            content,
            phase,
        },
        _ => unreachable!("user shell command output record should always be a message"),
    };

    if let Err(items) = session
        .inject_response_items(vec![response_input_item])
        .await
    {
        let response_items = items
            .into_iter()
            .map(ResponseItem::from)
            .collect::<Vec<_>>();
        session
            .record_conversation_items(turn_context, &response_items)
            .await;
    }
}
