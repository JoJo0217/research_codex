use codex_app_server_protocol::CommandExecutionStatus;

const OUTPUT_PREVIEW_CHAR_LIMIT: usize = 6000;

pub(crate) struct BackgroundTerminalCompletion<'a> {
    pub(crate) command_display: &'a str,
    pub(crate) status: &'a CommandExecutionStatus,
    pub(crate) exit_code: Option<i32>,
    pub(crate) output: &'a str,
}

pub(crate) fn completion_prompt(completion: BackgroundTerminalCompletion<'_>) -> String {
    let status = status_label(completion.status);
    let exit_code = completion
        .exit_code
        .map(|code| code.to_string())
        .unwrap_or_else(|| "unknown".to_string());
    let output = output_preview(completion.output);

    format!(
        "Background terminal completed.\n\nCommand: {}\nStatus: {status}\nExit code: {exit_code}\n\nOutput:\n{}\n\nContinue from this result. If it failed, diagnose the failure before starting another long-running command.",
        completion.command_display, output
    )
}

fn status_label(status: &CommandExecutionStatus) -> &'static str {
    match status {
        CommandExecutionStatus::InProgress => "in progress",
        CommandExecutionStatus::Completed => "completed",
        CommandExecutionStatus::Failed => "failed",
        CommandExecutionStatus::Declined => "declined",
    }
}

fn output_preview(output: &str) -> String {
    let output = output.trim();
    if output.is_empty() {
        return "(no output captured)".to_string();
    }

    let char_count = output.chars().count();
    if char_count <= OUTPUT_PREVIEW_CHAR_LIMIT {
        return output.to_string();
    }

    let head_count = OUTPUT_PREVIEW_CHAR_LIMIT / 2;
    let tail_count = OUTPUT_PREVIEW_CHAR_LIMIT - head_count;
    let head = output.chars().take(head_count).collect::<String>();
    let tail = output
        .chars()
        .rev()
        .take(tail_count)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect::<String>();
    let omitted = char_count.saturating_sub(OUTPUT_PREVIEW_CHAR_LIMIT);
    format!("{head}\n\n... {omitted} chars omitted ...\n\n{tail}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn completion_prompt_includes_command_status_exit_and_output() {
        let prompt = completion_prompt(BackgroundTerminalCompletion {
            command_display: "cargo test -p codex-tui",
            status: &CommandExecutionStatus::Completed,
            exit_code: Some(0),
            output: "ok\n",
        });

        assert!(prompt.contains("Command: cargo test -p codex-tui"));
        assert!(prompt.contains("Status: completed"));
        assert!(prompt.contains("Exit code: 0"));
        assert!(prompt.contains("Output:\nok"));
    }

    #[test]
    fn output_preview_limits_long_output() {
        let output = "a".repeat(OUTPUT_PREVIEW_CHAR_LIMIT + 10);
        let preview = output_preview(&output);

        assert!(preview.contains("... 10 chars omitted ..."));
        assert_eq!(
            preview.chars().filter(|ch| *ch == 'a').count(),
            OUTPUT_PREVIEW_CHAR_LIMIT
        );
    }
}
