//! Thread event buffering and replay state for the TUI app.
//!
//! This module owns the per-thread event store used when the TUI switches between the main
//! conversation, subagents, and side conversations. It keeps buffered app-server notifications,
//! pending interactive request replay state, active-turn tracking, and saved composer state close
//! together with the replay behavior that consumes them.

use super::*;
use std::collections::HashSet;

#[derive(Debug, Clone)]
pub(super) struct ThreadEventSnapshot {
    pub(super) session: Option<ThreadSessionState>,
    pub(super) turns: Vec<Turn>,
    pub(super) events: Vec<ThreadBufferedEvent>,
    pub(super) input_state: Option<ThreadInputState>,
}

#[derive(Debug, Clone)]
pub(super) enum ThreadBufferedEvent {
    Notification(ServerNotification),
    Request(ServerRequest),
    HistoryEntryResponse(HistoryLookupResponse),
    FeedbackSubmission(FeedbackThreadEvent),
}

#[derive(Debug, Default, PartialEq, Eq)]
pub(super) struct InactiveBackgroundTerminalNotificationResult {
    pub(super) wakeup_text: Option<String>,
    pub(super) drop_notification: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct FeedbackThreadEvent {
    pub(super) category: FeedbackCategory,
    pub(super) include_logs: bool,
    pub(super) feedback_audience: FeedbackAudience,
    pub(super) result: Result<String, String>,
}

#[derive(Debug)]
pub(super) struct ThreadEventStore {
    pub(super) session: Option<ThreadSessionState>,
    pub(super) turns: Vec<Turn>,
    pub(super) buffer: VecDeque<ThreadBufferedEvent>,
    pub(super) pending_interactive_replay: PendingInteractiveReplayState,
    pub(super) active_turn_id: Option<String>,
    pub(super) input_state: Option<ThreadInputState>,
    pub(super) capacity: usize,
    pub(super) active: bool,
    closed: bool,
    pending_offscreen_wakeup_prompts: VecDeque<String>,
    offscreen_wakeup_in_flight: bool,
    stopped_background_processes: HashSet<String>,
}

impl ThreadEventStore {
    pub(super) fn event_survives_session_refresh(event: &ThreadBufferedEvent) -> bool {
        matches!(
            event,
            ThreadBufferedEvent::Request(_)
                | ThreadBufferedEvent::Notification(ServerNotification::HookStarted(_))
                | ThreadBufferedEvent::Notification(ServerNotification::HookCompleted(_))
                | ThreadBufferedEvent::FeedbackSubmission(_)
        )
    }

    pub(super) fn new(capacity: usize) -> Self {
        Self {
            session: None,
            turns: Vec::new(),
            buffer: VecDeque::new(),
            pending_interactive_replay: PendingInteractiveReplayState::default(),
            active_turn_id: None,
            input_state: None,
            capacity,
            active: false,
            closed: false,
            pending_offscreen_wakeup_prompts: VecDeque::new(),
            offscreen_wakeup_in_flight: false,
            stopped_background_processes: HashSet::new(),
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) fn new_with_session(
        capacity: usize,
        session: ThreadSessionState,
        turns: Vec<Turn>,
    ) -> Self {
        let mut store = Self::new(capacity);
        store.session = Some(session);
        store.set_turns(turns);
        store
    }

    pub(super) fn set_session(&mut self, session: ThreadSessionState, turns: Vec<Turn>) {
        self.session = Some(session);
        self.closed = false;
        self.set_turns(turns);
    }

    pub(super) fn rebase_buffer_after_session_refresh(&mut self) {
        self.buffer.retain(Self::event_survives_session_refresh);
    }

    pub(super) fn set_turns(&mut self, turns: Vec<Turn>) {
        self.active_turn_id = turns
            .iter()
            .rev()
            .find(|turn| matches!(turn.status, TurnStatus::InProgress))
            .map(|turn| turn.id.clone());
        self.turns = turns;
    }

    pub(super) fn push_notification(&mut self, notification: ServerNotification) {
        if self.closed && closed_store_should_drop_late_notification(&notification) {
            return;
        }
        self.pending_interactive_replay
            .note_server_notification(&notification);
        match &notification {
            ServerNotification::TurnStarted(turn) => {
                self.active_turn_id = Some(turn.turn.id.clone());
                if let Some(input_state) = self.input_state.as_mut() {
                    input_state.clear_user_turn_pending_start_for_restore();
                }
            }
            ServerNotification::TurnCompleted(turn) => {
                if self.active_turn_id.as_deref() == Some(turn.turn.id.as_str()) {
                    self.active_turn_id = None;
                    self.offscreen_wakeup_in_flight = false;
                }
                if let Some(input_state) = self.input_state.as_mut() {
                    input_state.clear_user_turn_pending_start_for_restore();
                    input_state.mark_background_terminals_turn_completed(turn.turn.id.as_str());
                }
            }
            ServerNotification::ThreadClosed(_) => {
                self.closed = true;
                self.active_turn_id = None;
                self.offscreen_wakeup_in_flight = false;
                self.pending_offscreen_wakeup_prompts.clear();
                self.prune_closed_thread_state();
                if let Some(input_state) = self.input_state.as_mut() {
                    input_state.clear_user_turn_pending_start_for_restore();
                    input_state.clear_background_terminal_processes();
                }
            }
            _ => {}
        }
        if matches!(notification, ServerNotification::ItemCompleted(_)) {
            for identifier in background_terminal_identifiers_from_notification(&notification) {
                self.stopped_background_processes.remove(&identifier);
            }
        }
        self.buffer
            .push_back(ThreadBufferedEvent::Notification(notification));
        if self.buffer.len() > self.capacity
            && let Some(removed) = self.buffer.pop_front()
            && let ThreadBufferedEvent::Request(request) = &removed
        {
            self.pending_interactive_replay
                .note_evicted_server_request(request);
        }
    }

    pub(super) fn track_inactive_background_terminal_notification(
        &mut self,
        notification: &ServerNotification,
    ) -> InactiveBackgroundTerminalNotificationResult {
        if self.closed {
            return InactiveBackgroundTerminalNotificationResult {
                wakeup_text: None,
                drop_notification: closed_store_should_drop_late_notification(notification),
            };
        }
        if self.take_stopped_background_terminal_notification(notification) {
            return InactiveBackgroundTerminalNotificationResult {
                wakeup_text: None,
                drop_notification: true,
            };
        }
        if self.active {
            return InactiveBackgroundTerminalNotificationResult::default();
        }
        match notification {
            ServerNotification::ItemStarted(notification) => {
                let ThreadItem::CommandExecution {
                    id,
                    command,
                    process_id,
                    source,
                    ..
                } = &notification.item
                else {
                    return InactiveBackgroundTerminalNotificationResult::default();
                };
                if *source != codex_app_server_protocol::CommandExecutionSource::UnifiedExecStartup
                {
                    return InactiveBackgroundTerminalNotificationResult::default();
                }
                let input_state = self
                    .input_state
                    .get_or_insert_with(ThreadInputState::background_tracking_only);
                input_state.track_background_terminal_start(
                    id,
                    process_id.as_deref(),
                    command,
                    Some(notification.turn_id.as_str()),
                );
                InactiveBackgroundTerminalNotificationResult::default()
            }
            ServerNotification::CommandExecutionOutputDelta(notification) => {
                let Some(input_state) = self.input_state.as_mut() else {
                    return InactiveBackgroundTerminalNotificationResult::default();
                };
                input_state.track_background_terminal_output_chunk(
                    &notification.item_id,
                    notification.delta.as_bytes(),
                );
                InactiveBackgroundTerminalNotificationResult::default()
            }
            ServerNotification::ItemCompleted(notification) => {
                let ThreadItem::CommandExecution {
                    id,
                    process_id,
                    source,
                    status,
                    aggregated_output,
                    exit_code,
                    ..
                } = &notification.item
                else {
                    return InactiveBackgroundTerminalNotificationResult::default();
                };
                if !matches!(
                    *source,
                    codex_app_server_protocol::CommandExecutionSource::UnifiedExecStartup
                        | codex_app_server_protocol::CommandExecutionSource::UnifiedExecInteraction
                ) {
                    return InactiveBackgroundTerminalNotificationResult::default();
                }
                let process_identifiers =
                    background_terminal_identifiers_from_command(id, process_id.as_deref());
                let active_turn_id = self.active_turn_id.clone();
                let completion = {
                    let Some(input_state) = self.input_state.as_mut() else {
                        return InactiveBackgroundTerminalNotificationResult::default();
                    };
                    input_state.take_inactive_background_terminal_completion_wakeup(
                        id,
                        process_id.as_deref(),
                        status,
                        *exit_code,
                        aggregated_output.as_deref().unwrap_or_default(),
                        Some(notification.turn_id.as_str()),
                        active_turn_id.as_deref(),
                    )
                };
                let mut drop_notification = false;
                let wakeup_text = match completion {
                    Some(InactiveBackgroundTerminalCompletion::Wakeup(text)) => {
                        drop_notification = true;
                        self.queue_or_start_offscreen_wakeup(text)
                    }
                    Some(InactiveBackgroundTerminalCompletion::Handled) | None => None,
                };
                if drop_notification {
                    self.buffer.retain(|event| {
                        !background_terminal_event_matches_identifiers(event, &process_identifiers)
                    });
                }
                InactiveBackgroundTerminalNotificationResult {
                    wakeup_text,
                    drop_notification,
                }
            }
            _ => InactiveBackgroundTerminalNotificationResult::default(),
        }
    }

    fn has_busy_turn_or_wakeup(&self) -> bool {
        self.active_turn_id.is_some()
            || self.offscreen_wakeup_in_flight
            || self
                .input_state
                .as_ref()
                .is_some_and(ThreadInputState::user_turn_pending_start)
    }

    pub(super) fn queue_or_start_offscreen_wakeup(&mut self, text: String) -> Option<String> {
        if self.closed {
            return None;
        }
        if self.has_busy_turn_or_wakeup() {
            self.queue_offscreen_wakeup(text);
            return None;
        }
        self.offscreen_wakeup_in_flight = true;
        Some(text)
    }

    pub(super) fn queue_offscreen_wakeup(&mut self, text: String) {
        if self.closed {
            return;
        }
        self.pending_offscreen_wakeup_prompts.push_back(text);
    }

    pub(super) fn take_ready_offscreen_wakeup(&mut self) -> Option<String> {
        if self.closed {
            return None;
        }
        if self.has_busy_turn_or_wakeup() {
            return None;
        }
        let text = self.pending_offscreen_wakeup_prompts.pop_front()?;
        self.offscreen_wakeup_in_flight = true;
        Some(text)
    }

    pub(super) fn cancel_offscreen_wakeup_start(&mut self) {
        if self.active_turn_id.is_none() {
            self.offscreen_wakeup_in_flight = false;
        }
    }

    pub(super) fn mark_background_terminal_stopped(
        &mut self,
        process_id: &str,
        call_id: Option<&str>,
    ) {
        let identifiers = self.background_terminal_identifiers_for_process(process_id, call_id);
        for identifier in &identifiers {
            self.stopped_background_processes.insert(identifier.clone());
        }
        self.buffer
            .retain(|event| !background_terminal_event_matches_identifiers(event, &identifiers));
    }

    pub(super) fn mark_all_background_terminals_stopped(&mut self) {
        let identifiers = self.live_background_terminal_identifiers();
        for identifier in &identifiers {
            self.stopped_background_processes.insert(identifier.clone());
        }
        self.buffer
            .retain(|event| !background_terminal_event_matches_identifiers(event, &identifiers));
        if let Some(input_state) = self.input_state.as_mut() {
            input_state.clear_background_terminal_processes();
        }
    }

    fn live_background_terminal_identifiers(&self) -> Vec<String> {
        let mut live_groups: Vec<(Vec<String>, Vec<String>)> = Vec::new();
        for event in &self.buffer {
            let ThreadBufferedEvent::Notification(notification) = event else {
                continue;
            };
            let identifiers = background_terminal_identifiers_from_notification(notification);
            if identifiers.is_empty() {
                continue;
            }
            match notification {
                ServerNotification::ItemStarted(_) => {
                    let stable_identifiers =
                        background_terminal_stable_identifiers_from_notification(notification);
                    if !stable_identifiers.is_empty() {
                        live_groups.push((identifiers, stable_identifiers));
                    }
                }
                ServerNotification::ItemCompleted(_) => {
                    live_groups.retain(|group| {
                        !group
                            .0
                            .iter()
                            .any(|known| identifiers.iter().any(|identifier| identifier == known))
                    });
                }
                ServerNotification::CommandExecutionOutputDelta(_) => {}
                _ => {}
            }
        }
        let mut identifiers = Vec::new();
        for (_, stable_group) in live_groups {
            for identifier in stable_group {
                push_unique_identifier(&mut identifiers, identifier);
            }
        }
        identifiers
    }

    pub(super) fn take_stopped_background_terminal_notification(
        &mut self,
        notification: &ServerNotification,
    ) -> bool {
        let notification_identifiers =
            background_terminal_identifiers_from_notification(notification);
        if !notification_identifiers
            .iter()
            .any(|identifier| self.stopped_background_processes.contains(identifier))
        {
            return false;
        }
        if matches!(notification, ServerNotification::ItemCompleted(_)) {
            for identifier in
                background_terminal_reusable_identifiers_from_notification(notification)
            {
                self.stopped_background_processes.remove(&identifier);
            }
            if let Some(input_state) = self.input_state.as_mut() {
                for identifier in &notification_identifiers {
                    input_state.remove_background_terminal_process(identifier);
                }
            }
        }
        true
    }

    fn background_terminal_identifiers_for_process(
        &self,
        process_id: &str,
        call_id: Option<&str>,
    ) -> Vec<String> {
        let mut identifiers = Vec::new();
        if let Some(call_id) = call_id {
            push_unique_identifier(&mut identifiers, call_id);
        } else {
            push_unique_identifier(&mut identifiers, process_id);
        }
        for event in &self.buffer {
            let ThreadBufferedEvent::Notification(notification) = event else {
                continue;
            };
            let event_identifiers = background_terminal_identifiers_from_notification(notification);
            if event_identifiers
                .iter()
                .any(|identifier| identifiers.iter().any(|known| known == identifier))
            {
                for identifier in
                    background_terminal_stable_identifiers_from_notification(notification)
                {
                    push_unique_identifier(&mut identifiers, identifier);
                }
            }
        }
        identifiers
    }

    pub(super) fn mark_closed_for_liveness(&mut self) {
        self.closed = true;
        self.active_turn_id = None;
        self.offscreen_wakeup_in_flight = false;
        self.pending_offscreen_wakeup_prompts.clear();
        self.prune_closed_thread_state();
        if let Some(input_state) = self.input_state.as_mut() {
            input_state.clear_user_turn_pending_start_for_restore();
            input_state.clear_background_terminal_processes();
        }
    }

    fn prune_closed_thread_state(&mut self) {
        self.pending_interactive_replay = PendingInteractiveReplayState::default();
        self.buffer.retain(|event| {
            !matches!(event, ThreadBufferedEvent::Request(_))
                && !thread_event_is_background_terminal(event)
        });
    }

    pub(super) fn push_request(&mut self, request: ServerRequest) {
        self.pending_interactive_replay
            .note_server_request(&request);
        self.buffer.push_back(ThreadBufferedEvent::Request(request));
        if self.buffer.len() > self.capacity
            && let Some(removed) = self.buffer.pop_front()
            && let ThreadBufferedEvent::Request(request) = &removed
        {
            self.pending_interactive_replay
                .note_evicted_server_request(request);
        }
    }

    pub(super) fn pending_replay_requests(&self) -> Vec<ServerRequest> {
        if self.closed {
            return Vec::new();
        }
        self.buffer
            .iter()
            .filter_map(|event| match event {
                ThreadBufferedEvent::Request(request)
                    if self
                        .pending_interactive_replay
                        .should_replay_snapshot_request(request) =>
                {
                    Some(request.clone())
                }
                ThreadBufferedEvent::Request(_)
                | ThreadBufferedEvent::Notification(_)
                | ThreadBufferedEvent::HistoryEntryResponse(_)
                | ThreadBufferedEvent::FeedbackSubmission(_) => None,
            })
            .collect()
    }

    pub(super) fn file_change_changes(
        &self,
        turn_id: &str,
        item_id: &str,
    ) -> Option<Vec<codex_app_server_protocol::FileUpdateChange>> {
        self.buffer
            .iter()
            .rev()
            .find_map(|event| match event {
                ThreadBufferedEvent::Notification(ServerNotification::ItemStarted(
                    notification,
                )) if turn_id_matches(turn_id, &notification.turn_id) => {
                    file_change_item_changes(&notification.item, item_id)
                }
                ThreadBufferedEvent::Notification(ServerNotification::ItemCompleted(
                    notification,
                )) if turn_id_matches(turn_id, &notification.turn_id) => {
                    file_change_item_changes(&notification.item, item_id)
                }
                ThreadBufferedEvent::Request(_)
                | ThreadBufferedEvent::Notification(_)
                | ThreadBufferedEvent::HistoryEntryResponse(_)
                | ThreadBufferedEvent::FeedbackSubmission(_) => None,
            })
            .or_else(|| {
                self.turns
                    .iter()
                    .rev()
                    .filter(|turn| turn_id_matches(turn_id, &turn.id))
                    .flat_map(|turn| turn.items.iter().rev())
                    .find_map(|item| file_change_item_changes(item, item_id))
            })
    }

    pub(super) fn apply_thread_rollback(&mut self, response: &ThreadRollbackResponse) {
        self.turns = response.thread.turns.clone();
        self.buffer.clear();
        self.pending_interactive_replay = PendingInteractiveReplayState::default();
        self.active_turn_id = None;
        self.pending_offscreen_wakeup_prompts.clear();
        self.offscreen_wakeup_in_flight = false;
        self.closed = false;
    }

    pub(super) fn snapshot(&self) -> ThreadEventSnapshot {
        let mut input_state = self.input_state.clone();
        if !self.pending_offscreen_wakeup_prompts.is_empty() {
            let state = input_state.get_or_insert_with(ThreadInputState::background_tracking_only);
            for text in &self.pending_offscreen_wakeup_prompts {
                state.queue_background_terminal_completion_prompt_for_restore(text.clone());
            }
        }
        if self.offscreen_wakeup_in_flight {
            input_state
                .get_or_insert_with(ThreadInputState::background_tracking_only)
                .mark_user_turn_pending_start_for_restore();
        }
        ThreadEventSnapshot {
            session: self.session.clone(),
            turns: self.turns.clone(),
            // Thread switches replay buffered events into a rebuilt ChatWidget. Only replay
            // interactive prompts that are still pending, or answered approvals/input will reappear.
            events: self
                .buffer
                .iter()
                .filter(|event| match event {
                    ThreadBufferedEvent::Request(request) => self
                        .pending_interactive_replay
                        .should_replay_snapshot_request(request),
                    ThreadBufferedEvent::Notification(_)
                    | ThreadBufferedEvent::HistoryEntryResponse(_)
                    | ThreadBufferedEvent::FeedbackSubmission(_) => true,
                })
                .cloned()
                .collect(),
            input_state,
        }
    }

    pub(super) fn snapshot_for_activation(&mut self) -> ThreadEventSnapshot {
        let snapshot = self.snapshot();
        self.pending_offscreen_wakeup_prompts.clear();
        snapshot
    }

    pub(super) fn note_outbound_op<T>(&mut self, op: T)
    where
        T: Into<AppCommand>,
    {
        self.pending_interactive_replay.note_outbound_op(op);
    }

    pub(super) fn op_can_change_pending_replay_state<T>(op: T) -> bool
    where
        T: Into<AppCommand>,
    {
        PendingInteractiveReplayState::op_can_change_state(op)
    }

    pub(super) fn has_pending_thread_approvals(&self) -> bool {
        if self.closed {
            return false;
        }
        self.pending_interactive_replay
            .has_pending_thread_approvals()
    }

    pub(super) fn side_parent_pending_status(&self) -> Option<SideParentStatus> {
        if self.closed {
            return None;
        }
        if self
            .pending_interactive_replay
            .has_pending_thread_user_input()
        {
            Some(SideParentStatus::NeedsInput)
        } else if self
            .pending_interactive_replay
            .has_pending_thread_approvals()
        {
            Some(SideParentStatus::NeedsApproval)
        } else {
            None
        }
    }

    pub(super) fn active_turn_id(&self) -> Option<&str> {
        self.active_turn_id.as_deref()
    }

    pub(super) fn is_closed(&self) -> bool {
        self.closed
    }

    pub(super) fn clear_active_turn_id(&mut self) {
        self.active_turn_id = None;
    }
}

fn turn_id_matches(request_turn_id: &str, candidate_turn_id: &str) -> bool {
    request_turn_id.is_empty() || request_turn_id == candidate_turn_id
}

fn file_change_item_changes(
    item: &ThreadItem,
    item_id: &str,
) -> Option<Vec<codex_app_server_protocol::FileUpdateChange>> {
    match item {
        ThreadItem::FileChange { id, changes, .. } if id == item_id => Some(changes.clone()),
        _ => None,
    }
}

fn background_terminal_event_matches_identifiers(
    event: &ThreadBufferedEvent,
    identifiers: &[String],
) -> bool {
    let ThreadBufferedEvent::Notification(notification) = event else {
        return false;
    };
    background_terminal_identifiers_from_notification(notification)
        .iter()
        .any(|identifier| identifiers.iter().any(|known| known == identifier))
}

fn thread_event_is_background_terminal(event: &ThreadBufferedEvent) -> bool {
    let ThreadBufferedEvent::Notification(notification) = event else {
        return false;
    };
    closed_store_should_drop_notification(notification)
}

fn closed_store_should_drop_late_notification(notification: &ServerNotification) -> bool {
    !matches!(
        notification,
        ServerNotification::ThreadClosed(_)
            | ServerNotification::ThreadNameUpdated(_)
            | ServerNotification::ThreadTokenUsageUpdated(_)
            | ServerNotification::ThreadGoalUpdated(_)
            | ServerNotification::ThreadGoalCleared(_)
            | ServerNotification::ThreadArchived(_)
            | ServerNotification::ThreadUnarchived(_)
    )
}

fn closed_store_should_drop_notification(notification: &ServerNotification) -> bool {
    match notification {
        ServerNotification::ItemStarted(notification) => {
            command_execution_is_background_terminal(&notification.item)
        }
        ServerNotification::ItemCompleted(notification) => {
            command_execution_is_background_terminal(&notification.item)
        }
        ServerNotification::CommandExecutionOutputDelta(_) => true,
        _ => false,
    }
}

fn command_execution_is_background_terminal(item: &ThreadItem) -> bool {
    let ThreadItem::CommandExecution { source, .. } = item else {
        return false;
    };
    matches!(
        *source,
        codex_app_server_protocol::CommandExecutionSource::UnifiedExecStartup
            | codex_app_server_protocol::CommandExecutionSource::UnifiedExecInteraction
    )
}

fn push_unique_identifier(identifiers: &mut Vec<String>, identifier: impl Into<String>) {
    let identifier = identifier.into();
    if !identifiers.iter().any(|known| known == &identifier) {
        identifiers.push(identifier);
    }
}

fn background_terminal_identifiers_from_command(id: &str, process_id: Option<&str>) -> Vec<String> {
    let mut identifiers = Vec::new();
    push_unique_identifier(&mut identifiers, id);
    if let Some(process_id) = process_id {
        push_unique_identifier(&mut identifiers, process_id);
    }
    identifiers
}

fn background_terminal_identifiers_from_notification(
    notification: &ServerNotification,
) -> Vec<String> {
    match notification {
        ServerNotification::ItemStarted(notification) => {
            let ThreadItem::CommandExecution {
                id,
                process_id,
                source,
                ..
            } = &notification.item
            else {
                return Vec::new();
            };
            if !matches!(
                *source,
                codex_app_server_protocol::CommandExecutionSource::UnifiedExecStartup
                    | codex_app_server_protocol::CommandExecutionSource::UnifiedExecInteraction
            ) {
                return Vec::new();
            }
            background_terminal_identifiers_from_command(id, process_id.as_deref())
        }
        ServerNotification::ItemCompleted(notification) => {
            let ThreadItem::CommandExecution {
                id,
                process_id,
                source,
                ..
            } = &notification.item
            else {
                return Vec::new();
            };
            if !matches!(
                *source,
                codex_app_server_protocol::CommandExecutionSource::UnifiedExecStartup
                    | codex_app_server_protocol::CommandExecutionSource::UnifiedExecInteraction
            ) {
                return Vec::new();
            }
            background_terminal_identifiers_from_command(id, process_id.as_deref())
        }
        ServerNotification::CommandExecutionOutputDelta(notification) => {
            vec![notification.item_id.clone()]
        }
        _ => Vec::new(),
    }
}

fn background_terminal_stable_identifiers_from_notification(
    notification: &ServerNotification,
) -> Vec<String> {
    match notification {
        ServerNotification::ItemStarted(notification) => {
            background_terminal_stable_identifiers_from_item(&notification.item)
        }
        ServerNotification::ItemCompleted(notification) => {
            background_terminal_stable_identifiers_from_item(&notification.item)
        }
        ServerNotification::CommandExecutionOutputDelta(notification) => {
            vec![notification.item_id.clone()]
        }
        _ => Vec::new(),
    }
}

fn background_terminal_stable_identifiers_from_item(item: &ThreadItem) -> Vec<String> {
    let ThreadItem::CommandExecution { id, source, .. } = item else {
        return Vec::new();
    };
    if !matches!(
        *source,
        codex_app_server_protocol::CommandExecutionSource::UnifiedExecStartup
            | codex_app_server_protocol::CommandExecutionSource::UnifiedExecInteraction
    ) {
        return Vec::new();
    }
    vec![id.clone()]
}

fn background_terminal_reusable_identifiers_from_notification(
    notification: &ServerNotification,
) -> Vec<String> {
    match notification {
        ServerNotification::ItemStarted(notification) => {
            background_terminal_reusable_identifiers_from_item(&notification.item)
        }
        ServerNotification::ItemCompleted(notification) => {
            background_terminal_reusable_identifiers_from_item(&notification.item)
        }
        _ => Vec::new(),
    }
}

fn background_terminal_reusable_identifiers_from_item(item: &ThreadItem) -> Vec<String> {
    let ThreadItem::CommandExecution {
        process_id, source, ..
    } = item
    else {
        return Vec::new();
    };
    if !matches!(
        *source,
        codex_app_server_protocol::CommandExecutionSource::UnifiedExecStartup
            | codex_app_server_protocol::CommandExecutionSource::UnifiedExecInteraction
    ) {
        return Vec::new();
    }
    process_id.iter().cloned().collect()
}

#[derive(Debug)]
pub(super) struct ThreadEventChannel {
    pub(super) sender: mpsc::Sender<ThreadBufferedEvent>,
    pub(super) receiver: Option<mpsc::Receiver<ThreadBufferedEvent>>,
    pub(super) store: Arc<Mutex<ThreadEventStore>>,
}

impl ThreadEventChannel {
    pub(super) fn new(capacity: usize) -> Self {
        let (sender, receiver) = mpsc::channel(capacity);
        Self {
            sender,
            receiver: Some(receiver),
            store: Arc::new(Mutex::new(ThreadEventStore::new(capacity))),
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(super) fn new_with_session(
        capacity: usize,
        session: ThreadSessionState,
        turns: Vec<Turn>,
    ) -> Self {
        let (sender, receiver) = mpsc::channel(capacity);
        Self {
            sender,
            receiver: Some(receiver),
            store: Arc::new(Mutex::new(ThreadEventStore::new_with_session(
                capacity, session, turns,
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::PathBufExt;
    use crate::test_support::test_path_buf;
    use codex_app_server_protocol::AskForApproval;
    use codex_app_server_protocol::CommandExecutionRequestApprovalParams;
    use codex_app_server_protocol::HookCompletedNotification;
    use codex_app_server_protocol::HookEventName as AppServerHookEventName;
    use codex_app_server_protocol::HookExecutionMode as AppServerHookExecutionMode;
    use codex_app_server_protocol::HookHandlerType as AppServerHookHandlerType;
    use codex_app_server_protocol::HookOutputEntry as AppServerHookOutputEntry;
    use codex_app_server_protocol::HookOutputEntryKind as AppServerHookOutputEntryKind;
    use codex_app_server_protocol::HookRunStatus as AppServerHookRunStatus;
    use codex_app_server_protocol::HookRunSummary as AppServerHookRunSummary;
    use codex_app_server_protocol::HookScope as AppServerHookScope;
    use codex_app_server_protocol::HookStartedNotification;
    use codex_app_server_protocol::RequestId as AppServerRequestId;
    use codex_app_server_protocol::TurnCompletedNotification;
    use codex_app_server_protocol::TurnStartedNotification;
    use codex_config::types::ApprovalsReviewer;
    use codex_protocol::models::PermissionProfile;
    use pretty_assertions::assert_eq;
    use std::path::PathBuf;

    fn test_thread_session(thread_id: ThreadId, cwd: PathBuf) -> ThreadSessionState {
        ThreadSessionState {
            thread_id,
            forked_from_id: None,
            fork_parent_title: None,
            thread_name: None,
            model: "gpt-test".to_string(),
            model_provider_id: "test-provider".to_string(),
            service_tier: None,
            approval_policy: AskForApproval::Never,
            approvals_reviewer: ApprovalsReviewer::User,
            permission_profile: PermissionProfile::read_only(),
            active_permission_profile: None,
            cwd: cwd.abs(),
            discovery_cwd: None,
            instruction_source_paths: Vec::new(),
            reasoning_effort: None,
            message_history: None,
            network_proxy: None,
            rollout_path: Some(PathBuf::new()),
        }
    }

    fn test_turn(turn_id: &str, status: TurnStatus, items: Vec<ThreadItem>) -> Turn {
        Turn {
            id: turn_id.to_string(),
            items_view: codex_app_server_protocol::TurnItemsView::Full,
            items,
            status,
            error: None,
            started_at: None,
            completed_at: None,
            duration_ms: None,
        }
    }

    fn turn_started_notification(thread_id: ThreadId, turn_id: &str) -> ServerNotification {
        ServerNotification::TurnStarted(TurnStartedNotification {
            thread_id: thread_id.to_string(),
            turn: Turn {
                started_at: Some(0),
                ..test_turn(turn_id, TurnStatus::InProgress, Vec::new())
            },
        })
    }

    fn turn_completed_notification(
        thread_id: ThreadId,
        turn_id: &str,
        status: TurnStatus,
    ) -> ServerNotification {
        ServerNotification::TurnCompleted(TurnCompletedNotification {
            thread_id: thread_id.to_string(),
            turn: Turn {
                completed_at: Some(0),
                duration_ms: Some(1),
                ..test_turn(turn_id, status, Vec::new())
            },
        })
    }

    fn hook_started_notification(thread_id: ThreadId, turn_id: &str) -> ServerNotification {
        ServerNotification::HookStarted(HookStartedNotification {
            thread_id: thread_id.to_string(),
            turn_id: Some(turn_id.to_string()),
            run: AppServerHookRunSummary {
                id: "user-prompt-submit:0:/tmp/hooks.json".to_string(),
                event_name: AppServerHookEventName::UserPromptSubmit,
                handler_type: AppServerHookHandlerType::Command,
                execution_mode: AppServerHookExecutionMode::Sync,
                scope: AppServerHookScope::Turn,
                source_path: test_path_buf("/tmp/hooks.json").abs(),
                source: codex_app_server_protocol::HookSource::User,
                display_order: 0,
                status: AppServerHookRunStatus::Running,
                status_message: Some("checking go-workflow input policy".to_string()),
                started_at: 1,
                completed_at: None,
                duration_ms: None,
                entries: Vec::new(),
            },
        })
    }

    fn hook_completed_notification(thread_id: ThreadId, turn_id: &str) -> ServerNotification {
        ServerNotification::HookCompleted(HookCompletedNotification {
            thread_id: thread_id.to_string(),
            turn_id: Some(turn_id.to_string()),
            run: AppServerHookRunSummary {
                id: "user-prompt-submit:0:/tmp/hooks.json".to_string(),
                event_name: AppServerHookEventName::UserPromptSubmit,
                handler_type: AppServerHookHandlerType::Command,
                execution_mode: AppServerHookExecutionMode::Sync,
                scope: AppServerHookScope::Turn,
                source_path: test_path_buf("/tmp/hooks.json").abs(),
                source: codex_app_server_protocol::HookSource::User,
                display_order: 0,
                status: AppServerHookRunStatus::Stopped,
                status_message: Some("checking go-workflow input policy".to_string()),
                started_at: 1,
                completed_at: Some(11),
                duration_ms: Some(10),
                entries: vec![
                    AppServerHookOutputEntry {
                        kind: AppServerHookOutputEntryKind::Warning,
                        text: "go-workflow must start from PlanMode".to_string(),
                    },
                    AppServerHookOutputEntry {
                        kind: AppServerHookOutputEntryKind::Stop,
                        text: "prompt blocked".to_string(),
                    },
                ],
            },
        })
    }

    fn exec_approval_request(
        thread_id: ThreadId,
        turn_id: &str,
        item_id: &str,
        approval_id: Option<&str>,
    ) -> ServerRequest {
        ServerRequest::CommandExecutionRequestApproval {
            request_id: AppServerRequestId::Integer(1),
            params: CommandExecutionRequestApprovalParams {
                thread_id: thread_id.to_string(),
                turn_id: turn_id.to_string(),
                item_id: item_id.to_string(),
                started_at_ms: 0,
                approval_id: approval_id.map(str::to_string),
                reason: Some("needs approval".to_string()),
                network_approval_context: None,
                command: Some("echo hello".to_string()),
                cwd: Some(test_path_buf("/tmp/project").abs()),
                command_actions: None,
                additional_permissions: None,
                proposed_execpolicy_amendment: None,
                proposed_network_policy_amendments: None,
                available_decisions: None,
            },
        }
    }

    fn unified_exec_item(
        call_id: &str,
        process_id: &str,
        status: codex_app_server_protocol::CommandExecutionStatus,
        output: Option<&str>,
    ) -> ThreadItem {
        ThreadItem::CommandExecution {
            id: call_id.to_string(),
            command: "bash -lc 'sleep 1'".to_string(),
            cwd: test_path_buf("/tmp/project").abs(),
            process_id: Some(process_id.to_string()),
            source: codex_app_server_protocol::CommandExecutionSource::UnifiedExecStartup,
            status,
            command_actions: Vec::new(),
            aggregated_output: output.map(str::to_string),
            exit_code: Some(0),
            duration_ms: Some(5),
        }
    }

    fn unified_exec_started_notification(
        thread_id: ThreadId,
        turn_id: &str,
        call_id: &str,
        process_id: &str,
    ) -> ServerNotification {
        ServerNotification::ItemStarted(codex_app_server_protocol::ItemStartedNotification {
            thread_id: thread_id.to_string(),
            turn_id: turn_id.to_string(),
            started_at_ms: 0,
            item: unified_exec_item(
                call_id,
                process_id,
                codex_app_server_protocol::CommandExecutionStatus::InProgress,
                None,
            ),
        })
    }

    fn unified_exec_completed_notification(
        thread_id: ThreadId,
        turn_id: &str,
        call_id: &str,
        process_id: &str,
    ) -> ServerNotification {
        ServerNotification::ItemCompleted(codex_app_server_protocol::ItemCompletedNotification {
            thread_id: thread_id.to_string(),
            turn_id: turn_id.to_string(),
            completed_at_ms: 1,
            item: unified_exec_item(
                call_id,
                process_id,
                codex_app_server_protocol::CommandExecutionStatus::Completed,
                Some("done\n"),
            ),
        })
    }

    fn thread_closed_notification(thread_id: ThreadId) -> ServerNotification {
        ServerNotification::ThreadClosed(codex_app_server_protocol::ThreadClosedNotification {
            thread_id: thread_id.to_string(),
        })
    }

    #[test]
    fn thread_event_store_tracks_active_turn_lifecycle() {
        let mut store = ThreadEventStore::new(/*capacity*/ 8);
        assert_eq!(store.active_turn_id(), None);

        let thread_id = ThreadId::new();
        store.push_notification(turn_started_notification(thread_id, "turn-1"));
        assert_eq!(store.active_turn_id(), Some("turn-1"));

        store.push_notification(turn_completed_notification(
            thread_id,
            "turn-2",
            TurnStatus::Completed,
        ));
        assert_eq!(store.active_turn_id(), Some("turn-1"));

        store.push_notification(turn_completed_notification(
            thread_id,
            "turn-1",
            TurnStatus::Interrupted,
        ));
        assert_eq!(store.active_turn_id(), None);
    }

    #[test]
    fn inactive_store_tracks_never_opened_background_terminal() {
        let mut store = ThreadEventStore::new(/*capacity*/ 8);
        let thread_id = ThreadId::new();

        store.push_notification(turn_started_notification(thread_id, "turn-1"));
        assert_eq!(
            store.track_inactive_background_terminal_notification(
                &unified_exec_started_notification(thread_id, "turn-1", "call-bg", "process-bg")
            ),
            InactiveBackgroundTerminalNotificationResult::default()
        );
        let input_state = store.input_state.as_ref().expect("input state created");
        assert_eq!(
            input_state
                .background_terminal_activity_summaries(thread_id)
                .len(),
            1
        );

        store.push_notification(turn_completed_notification(
            thread_id,
            "turn-1",
            TurnStatus::Completed,
        ));
        let result = store.track_inactive_background_terminal_notification(
            &unified_exec_completed_notification(thread_id, "turn-1", "call-bg", "process-bg"),
        );
        assert!(result.wakeup_text.is_some());
        assert!(result.drop_notification);
        let input_state = store.input_state.as_ref().expect("input state retained");
        assert!(
            input_state
                .background_terminal_activity_summaries(thread_id)
                .is_empty()
        );
    }

    #[test]
    fn offscreen_wakeup_prompts_are_serialized_until_turn_completes() {
        let mut store = ThreadEventStore::new(/*capacity*/ 8);
        let thread_id = ThreadId::new();

        assert_eq!(
            store.queue_or_start_offscreen_wakeup("first".to_string()),
            Some("first".to_string())
        );
        assert_eq!(
            store.queue_or_start_offscreen_wakeup("second".to_string()),
            None
        );
        store.push_notification(turn_started_notification(thread_id, "turn-wakeup"));
        assert_eq!(store.take_ready_offscreen_wakeup(), None);
        store.push_notification(turn_completed_notification(
            thread_id,
            "turn-wakeup",
            TurnStatus::Completed,
        ));
        assert_eq!(
            store.take_ready_offscreen_wakeup(),
            Some("second".to_string())
        );
    }

    #[test]
    fn offscreen_wakeup_waits_for_pending_turn_start() {
        let mut store = ThreadEventStore::new(/*capacity*/ 8);
        let thread_id = ThreadId::new();
        let mut input_state = ThreadInputState::background_tracking_only();
        input_state.mark_user_turn_pending_start_for_restore();
        store.input_state = Some(input_state);

        assert_eq!(
            store.queue_or_start_offscreen_wakeup("queued".to_string()),
            None
        );
        assert_eq!(store.take_ready_offscreen_wakeup(), None);

        store.push_notification(turn_started_notification(thread_id, "turn-pending"));
        assert_eq!(store.take_ready_offscreen_wakeup(), None);

        store.push_notification(turn_completed_notification(
            thread_id,
            "turn-pending",
            TurnStatus::Completed,
        ));
        assert_eq!(
            store.take_ready_offscreen_wakeup(),
            Some("queued".to_string())
        );
    }

    #[test]
    fn thread_closed_discards_pending_offscreen_wakeups() {
        let mut store = ThreadEventStore::new(/*capacity*/ 8);
        let thread_id = ThreadId::new();

        assert_eq!(
            store.queue_or_start_offscreen_wakeup("first".to_string()),
            Some("first".to_string())
        );
        assert_eq!(
            store.queue_or_start_offscreen_wakeup("second".to_string()),
            None
        );

        store.push_notification(thread_closed_notification(thread_id));

        assert_eq!(store.take_ready_offscreen_wakeup(), None);
    }

    #[test]
    fn thread_closed_clears_tracked_background_terminals() {
        let mut store = ThreadEventStore::new(/*capacity*/ 8);
        let thread_id = ThreadId::new();

        store.track_inactive_background_terminal_notification(&unified_exec_started_notification(
            thread_id, "turn-1", "call-bg", "proc-bg",
        ));
        assert_eq!(
            store
                .input_state
                .as_ref()
                .expect("input state")
                .background_terminal_activity_summaries(thread_id)
                .len(),
            1
        );

        store.push_notification(thread_closed_notification(thread_id));

        assert!(
            store
                .input_state
                .as_ref()
                .expect("input state")
                .background_terminal_activity_summaries(thread_id)
                .is_empty()
        );
    }

    #[test]
    fn thread_closed_prunes_buffered_background_terminal_events() {
        let mut store = ThreadEventStore::new(/*capacity*/ 8);
        let thread_id = ThreadId::new();

        store.push_notification(turn_started_notification(thread_id, "turn-1"));
        store.push_notification(unified_exec_started_notification(
            thread_id, "turn-1", "call-bg", "proc-bg",
        ));
        store.push_notification(hook_started_notification(thread_id, "turn-1"));
        store.push_notification(thread_closed_notification(thread_id));

        let snapshot = store.snapshot();
        assert!(
            snapshot
                .events
                .iter()
                .all(|event| !thread_event_is_background_terminal(event))
        );
        assert!(snapshot.events.iter().any(|event| matches!(
            event,
            ThreadBufferedEvent::Notification(ServerNotification::HookStarted(_))
        )));
    }

    #[test]
    fn liveness_close_clears_pending_interactive_requests() {
        let mut store = ThreadEventStore::new(/*capacity*/ 8);
        let thread_id = ThreadId::new();

        store.push_request(exec_approval_request(
            thread_id,
            "turn-1",
            "call-approval",
            Some("approval-1"),
        ));
        assert!(store.has_pending_thread_approvals());
        assert_eq!(store.pending_replay_requests().len(), 1);

        store.mark_closed_for_liveness();

        assert!(!store.has_pending_thread_approvals());
        assert!(store.pending_replay_requests().is_empty());
        assert!(store.side_parent_pending_status().is_none());
        assert!(
            store
                .snapshot()
                .events
                .iter()
                .all(|event| !matches!(event, ThreadBufferedEvent::Request(_)))
        );
    }

    #[test]
    fn closed_store_ignores_late_background_terminal_events() {
        let mut store = ThreadEventStore::new(/*capacity*/ 8);
        let thread_id = ThreadId::new();
        store.mark_closed_for_liveness();

        let start_result = store.track_inactive_background_terminal_notification(
            &unified_exec_started_notification(thread_id, "turn-1", "call-bg", "proc-bg"),
        );
        let output_result = store.track_inactive_background_terminal_notification(
            &ServerNotification::CommandExecutionOutputDelta(
                codex_app_server_protocol::CommandExecutionOutputDeltaNotification {
                    thread_id: thread_id.to_string(),
                    turn_id: "turn-1".to_string(),
                    item_id: "call-bg".to_string(),
                    delta: "late output".to_string(),
                },
            ),
        );
        let completed_result = store.track_inactive_background_terminal_notification(
            &unified_exec_completed_notification(thread_id, "turn-1", "call-bg", "proc-bg"),
        );

        assert!(start_result.drop_notification);
        assert!(output_result.drop_notification);
        assert!(completed_result.drop_notification);
        assert!(store.input_state.as_ref().is_none_or(|state| {
            state
                .background_terminal_activity_summaries(thread_id)
                .is_empty()
        }));
        assert_eq!(
            store.queue_or_start_offscreen_wakeup("late wakeup".to_string()),
            None
        );
        assert_eq!(store.take_ready_offscreen_wakeup(), None);
    }

    #[test]
    fn closed_store_ignores_late_turn_started() {
        let mut store = ThreadEventStore::new(/*capacity*/ 8);
        let thread_id = ThreadId::new();
        store.mark_closed_for_liveness();

        store.push_notification(turn_started_notification(thread_id, "late-turn"));

        assert!(store.is_closed());
        assert_eq!(store.active_turn_id(), None);
        assert!(store.snapshot().events.iter().all(|event| !matches!(
            event,
            ThreadBufferedEvent::Notification(ServerNotification::TurnStarted(_))
        )));
    }

    #[test]
    fn busy_inactive_completion_is_dropped_and_pruned_when_wakeup_queued() {
        let mut store = ThreadEventStore::new(/*capacity*/ 8);
        let thread_id = ThreadId::new();

        store.push_notification(turn_started_notification(thread_id, "turn-1"));
        let started = unified_exec_started_notification(thread_id, "turn-1", "call-bg", "proc-bg");
        store.push_notification(started.clone());
        assert_eq!(
            store.track_inactive_background_terminal_notification(&started),
            InactiveBackgroundTerminalNotificationResult::default()
        );
        store.push_notification(turn_completed_notification(
            thread_id,
            "turn-1",
            TurnStatus::Completed,
        ));
        assert_eq!(
            store.queue_or_start_offscreen_wakeup("in flight".to_string()),
            Some("in flight".to_string())
        );

        let output = ServerNotification::CommandExecutionOutputDelta(
            codex_app_server_protocol::CommandExecutionOutputDeltaNotification {
                thread_id: thread_id.to_string(),
                turn_id: "turn-1".to_string(),
                item_id: "call-bg".to_string(),
                delta: "queued output".to_string(),
            },
        );
        store.push_notification(output);
        let result = store.track_inactive_background_terminal_notification(
            &unified_exec_completed_notification(thread_id, "turn-1", "call-bg", "proc-bg"),
        );

        assert_eq!(
            result,
            InactiveBackgroundTerminalNotificationResult {
                wakeup_text: None,
                drop_notification: true,
            }
        );
        assert!(store.snapshot().events.iter().all(|event| {
            !background_terminal_event_matches_identifiers(
                event,
                &["proc-bg".to_string(), "call-bg".to_string()],
            )
        }));
    }

    #[test]
    fn stopped_background_terminal_start_is_pruned_from_replay_buffer() {
        let mut store = ThreadEventStore::new(/*capacity*/ 8);
        let thread_id = ThreadId::new();
        let started = unified_exec_started_notification(thread_id, "turn-1", "call-bg", "proc-bg");

        store.push_notification(started);
        store.push_notification(ServerNotification::CommandExecutionOutputDelta(
            codex_app_server_protocol::CommandExecutionOutputDeltaNotification {
                thread_id: thread_id.to_string(),
                turn_id: "turn-1".to_string(),
                item_id: "call-bg".to_string(),
                delta: "output before stop".to_string(),
            },
        ));
        assert_eq!(store.snapshot().events.len(), 2);
        store.mark_background_terminal_stopped("proc-bg", Some("call-bg"));
        assert!(store.snapshot().events.is_empty());
        let start_result = store.track_inactive_background_terminal_notification(
            &unified_exec_started_notification(thread_id, "turn-1", "call-bg", "proc-bg"),
        );
        assert!(start_result.drop_notification);
        let completed =
            unified_exec_completed_notification(thread_id, "turn-1", "call-bg", "proc-bg");
        let result = store.track_inactive_background_terminal_notification(&completed);
        assert!(result.drop_notification);
    }

    #[test]
    fn mark_all_background_terminals_stopped_prunes_output_and_drops_late_completion() {
        let mut store = ThreadEventStore::new(/*capacity*/ 8);
        let thread_id = ThreadId::new();
        store.push_notification(unified_exec_started_notification(
            thread_id,
            "turn-0",
            "call-done",
            "proc-done",
        ));
        store.push_notification(unified_exec_completed_notification(
            thread_id,
            "turn-0",
            "call-done",
            "proc-done",
        ));
        store.push_notification(unified_exec_started_notification(
            thread_id, "turn-1", "call-bg", "proc-bg",
        ));
        store.push_notification(ServerNotification::CommandExecutionOutputDelta(
            codex_app_server_protocol::CommandExecutionOutputDeltaNotification {
                thread_id: thread_id.to_string(),
                turn_id: "turn-1".to_string(),
                item_id: "call-bg".to_string(),
                delta: "output before stop".to_string(),
            },
        ));

        store.mark_all_background_terminals_stopped();

        assert!(store.snapshot().events.is_empty());
        let late_output = store.track_inactive_background_terminal_notification(
            &ServerNotification::CommandExecutionOutputDelta(
                codex_app_server_protocol::CommandExecutionOutputDeltaNotification {
                    thread_id: thread_id.to_string(),
                    turn_id: "turn-1".to_string(),
                    item_id: "call-bg".to_string(),
                    delta: "late output".to_string(),
                },
            ),
        );
        assert!(late_output.drop_notification);
        let late_completion = store.track_inactive_background_terminal_notification(
            &unified_exec_completed_notification(thread_id, "turn-1", "call-bg", "proc-bg"),
        );
        assert!(late_completion.drop_notification);
        let stale_queued_start = store.track_inactive_background_terminal_notification(
            &unified_exec_started_notification(thread_id, "turn-1", "call-bg", "proc-bg"),
        );
        assert!(stale_queued_start.drop_notification);
        let stale_queued_output = store.track_inactive_background_terminal_notification(
            &ServerNotification::CommandExecutionOutputDelta(
                codex_app_server_protocol::CommandExecutionOutputDeltaNotification {
                    thread_id: thread_id.to_string(),
                    turn_id: "turn-1".to_string(),
                    item_id: "call-bg".to_string(),
                    delta: "queued output after completion".to_string(),
                },
            ),
        );
        assert!(stale_queued_output.drop_notification);
        let reused_stopped_pid_start = store.track_inactive_background_terminal_notification(
            &unified_exec_started_notification(thread_id, "turn-2", "call-new", "proc-bg"),
        );
        assert!(!reused_stopped_pid_start.drop_notification);
        let reused_completed_id_start = store.track_inactive_background_terminal_notification(
            &unified_exec_started_notification(thread_id, "turn-2", "call-done-2", "proc-done"),
        );
        assert!(!reused_completed_id_start.drop_notification);
    }

    #[test]
    fn thread_event_store_restores_active_turn_from_snapshot_turns() {
        let thread_id = ThreadId::new();
        let session = test_thread_session(thread_id, test_path_buf("/tmp/project"));
        let turns = vec![
            test_turn("turn-1", TurnStatus::Completed, Vec::new()),
            test_turn("turn-2", TurnStatus::InProgress, Vec::new()),
        ];

        let store =
            ThreadEventStore::new_with_session(/*capacity*/ 8, session.clone(), turns.clone());
        assert_eq!(store.active_turn_id(), Some("turn-2"));

        let mut refreshed_store = ThreadEventStore::new(/*capacity*/ 8);
        refreshed_store.set_session(session, turns);
        assert_eq!(refreshed_store.active_turn_id(), Some("turn-2"));
    }

    #[test]
    fn thread_event_store_clear_active_turn_id_resets_cached_turn() {
        let mut store = ThreadEventStore::new(/*capacity*/ 8);
        let thread_id = ThreadId::new();
        store.push_notification(turn_started_notification(thread_id, "turn-1"));

        store.clear_active_turn_id();

        assert_eq!(store.active_turn_id(), None);
    }

    #[test]
    fn thread_event_store_rebase_preserves_resolved_request_state() {
        let thread_id = ThreadId::new();
        let mut store = ThreadEventStore::new(/*capacity*/ 8);
        store.push_request(exec_approval_request(
            thread_id,
            "turn-approval",
            "call-approval",
            /*approval_id*/ None,
        ));
        store.push_notification(ServerNotification::ServerRequestResolved(
            codex_app_server_protocol::ServerRequestResolvedNotification {
                request_id: AppServerRequestId::Integer(1),
                thread_id: thread_id.to_string(),
            },
        ));

        store.rebase_buffer_after_session_refresh();

        let snapshot = store.snapshot();
        assert!(snapshot.events.is_empty());
        assert_eq!(store.has_pending_thread_approvals(), false);
    }

    #[test]
    fn thread_event_store_rebase_preserves_hook_notifications() {
        let thread_id = ThreadId::new();
        let mut store = ThreadEventStore::new(/*capacity*/ 8);
        store.push_notification(hook_started_notification(thread_id, "turn-hook"));
        store.push_notification(hook_completed_notification(thread_id, "turn-hook"));

        store.rebase_buffer_after_session_refresh();

        let snapshot = store.snapshot();
        let hook_notifications = snapshot
            .events
            .into_iter()
            .map(|event| match event {
                ThreadBufferedEvent::Notification(notification) => {
                    serde_json::to_value(notification).expect("hook notification should serialize")
                }
                other => panic!("expected buffered hook notification, saw: {other:?}"),
            })
            .collect::<Vec<_>>();
        assert_eq!(
            hook_notifications,
            vec![
                serde_json::to_value(hook_started_notification(thread_id, "turn-hook"))
                    .expect("hook notification should serialize"),
                serde_json::to_value(hook_completed_notification(thread_id, "turn-hook"))
                    .expect("hook notification should serialize"),
            ]
        );
    }
}
