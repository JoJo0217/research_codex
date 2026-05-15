//! Thread routing, buffering, and app-server operation submission for the TUI app.
//!
//! This module manages active thread channels, routes server requests and notifications into those
//! channels, submits thread-scoped operations through the app server, and replays buffered events
//! when the visible thread changes.

use super::*;
use crate::session_resume::read_session_model;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ThreadRequestEnqueueResult {
    Enqueued,
    ThreadClosed,
}

impl App {
    pub(super) async fn shutdown_current_thread(&mut self, app_server: &mut AppServerSession) {
        if let Some(thread_id) = self.chat_widget.thread_id() {
            // Clear any in-flight rollback guard when switching threads.
            self.discard_pending_backtrack_rollback();
            if let Err(err) = app_server.thread_unsubscribe(thread_id).await {
                tracing::warn!("failed to unsubscribe thread {thread_id}: {err}");
            }
            self.abort_thread_event_listener(thread_id);
        }
    }

    pub(super) fn abort_thread_event_listener(&mut self, thread_id: ThreadId) {
        if let Some(handle) = self.thread_event_listener_tasks.remove(&thread_id) {
            handle.abort();
        }
    }

    pub(super) fn abort_all_thread_event_listeners(&mut self) {
        for handle in self
            .thread_event_listener_tasks
            .drain()
            .map(|(_, handle)| handle)
        {
            handle.abort();
        }
    }

    pub(super) fn ensure_thread_channel(&mut self, thread_id: ThreadId) -> &mut ThreadEventChannel {
        self.thread_event_channels
            .entry(thread_id)
            .or_insert_with(|| ThreadEventChannel::new(THREAD_EVENT_CHANNEL_CAPACITY))
    }

    pub(super) async fn set_thread_active(&mut self, thread_id: ThreadId, active: bool) {
        if let Some(channel) = self.thread_event_channels.get_mut(&thread_id) {
            let mut store = channel.store.lock().await;
            store.active = active;
        }
    }

    pub(super) async fn sync_background_terminal_activity_summaries(&mut self) {
        let active_thread_id = self.active_thread_id;
        let mut summaries: Vec<BackgroundTerminalActivitySummary> = Vec::new();
        if let Some(thread_id) = active_thread_id {
            summaries.extend(
                self.chat_widget
                    .current_background_terminal_activity_summaries(thread_id),
            );
        }

        let stores = self
            .thread_event_channels
            .iter()
            .filter_map(|(thread_id, channel)| {
                (Some(*thread_id) != active_thread_id)
                    .then_some((*thread_id, Arc::clone(&channel.store)))
            })
            .collect::<Vec<_>>();
        for (thread_id, store) in stores {
            let guard = store.lock().await;
            if guard.is_closed() {
                continue;
            }
            if let Some(input_state) = guard.input_state.as_ref() {
                summaries.extend(input_state.background_terminal_activity_summaries(thread_id));
            }
        }
        self.chat_widget
            .set_background_terminal_activity_summaries(summaries);
    }

    #[cfg(test)]
    pub(super) async fn mark_active_background_terminals_stopped(&mut self) {
        let Some(thread_id) = self.active_thread_id else {
            return;
        };
        self.mark_background_terminals_stopped_for_thread(thread_id)
            .await;
    }

    pub(super) async fn mark_background_terminals_stopped_for_thread(
        &mut self,
        thread_id: ThreadId,
    ) {
        if let Some(store) = self
            .thread_event_channels
            .get(&thread_id)
            .map(|channel| Arc::clone(&channel.store))
        {
            store.lock().await.mark_all_background_terminals_stopped();
        }
        self.sync_background_terminal_activity_summaries().await;
    }

    pub(super) async fn should_drop_stopped_background_terminal_event(
        &mut self,
        event: &ThreadBufferedEvent,
    ) -> bool {
        let ThreadBufferedEvent::Notification(notification) = event else {
            return false;
        };
        let Some(thread_id) = self.active_thread_id else {
            return false;
        };
        let Some(store) = self
            .thread_event_channels
            .get(&thread_id)
            .map(|channel| Arc::clone(&channel.store))
        else {
            return false;
        };
        store
            .lock()
            .await
            .take_stopped_background_terminal_notification(notification)
    }

    pub(super) async fn stop_background_terminal(
        &mut self,
        app_server: &mut AppServerSession,
        thread_id: ThreadId,
        process_id: String,
    ) -> Result<()> {
        let (command_display, call_id) = if self.active_thread_id == Some(thread_id) {
            (
                self.chat_widget
                    .background_terminal_command_display(&process_id),
                self.chat_widget.background_terminal_call_id(&process_id),
            )
        } else if let Some(store) = self
            .thread_event_channels
            .get(&thread_id)
            .map(|channel| Arc::clone(&channel.store))
        {
            let guard = store.lock().await;
            let command_display = guard
                .input_state
                .as_ref()
                .and_then(|state| state.background_terminal_command_display(&process_id));
            let call_id = guard
                .input_state
                .as_ref()
                .and_then(|state| state.background_terminal_call_id(&process_id));
            (command_display, call_id)
        } else {
            (None, None)
        };
        let Some(command_display) = command_display else {
            self.chat_widget
                .add_error_message("Background terminal is no longer running.".to_string());
            self.sync_background_terminal_activity_summaries().await;
            return Ok(());
        };

        self.submit_thread_op(
            app_server,
            thread_id,
            AppCommand::terminate_background_terminal(process_id.clone()),
        )
        .await?;
        if let Some(store) = self
            .thread_event_channels
            .get(&thread_id)
            .map(|channel| Arc::clone(&channel.store))
        {
            let mut guard = store.lock().await;
            guard.mark_background_terminal_stopped(&process_id, call_id.as_deref());
        }
        if self.active_thread_id == Some(thread_id) {
            self.chat_widget
                .remove_background_terminal_process(&process_id);
        } else if let Some(store) = self
            .thread_event_channels
            .get(&thread_id)
            .map(|channel| Arc::clone(&channel.store))
        {
            let mut guard = store.lock().await;
            if let Some(input_state) = guard.input_state.as_mut() {
                input_state.remove_background_terminal_process(&process_id);
            }
        }
        self.sync_background_terminal_activity_summaries().await;
        self.chat_widget.add_info_message(
            format!("Stopping background terminal: {command_display}"),
            /*hint*/ None,
        );
        Ok(())
    }

    fn plain_text_wakeup_command(session: &ThreadSessionState, text: String) -> AppCommand {
        AppCommand::UserTurn {
            items: vec![UserInput::Text {
                text,
                text_elements: Vec::new(),
            }],
            cwd: session.cwd.to_path_buf(),
            approval_policy: session.approval_policy,
            approvals_reviewer: Some(session.approvals_reviewer),
            permission_profile: session.permission_profile.clone(),
            model: session.model.clone(),
            effort: session.reasoning_effort,
            summary: None,
            service_tier: session.service_tier.clone().map(Some),
            final_output_json_schema: None,
            collaboration_mode: None,
            personality: None,
        }
    }

    fn subagent_completion_wakeup_prompt(label: &str, thread_id: ThreadId) -> String {
        format!(
            "Subagent completed.\n\nAgent: {label}\nThread: {thread_id}\n\nContinue from this subagent result. Inspect the subagent transcript if needed before taking further action."
        )
    }

    pub(super) async fn activate_thread_channel(&mut self, thread_id: ThreadId) {
        if self.active_thread_id.is_some() {
            return;
        }
        self.set_thread_active(thread_id, /*active*/ true).await;
        let receiver = if let Some(channel) = self.thread_event_channels.get_mut(&thread_id) {
            channel.receiver.take()
        } else {
            None
        };
        self.active_thread_id = Some(thread_id);
        self.active_thread_rx = receiver;
        self.refresh_pending_thread_approvals().await;
    }

    pub(super) async fn store_active_thread_receiver(&mut self) {
        let Some(active_id) = self.active_thread_id else {
            return;
        };
        let mut input_state = self.chat_widget.capture_thread_input_state();
        let turn_start_pending = input_state
            .as_ref()
            .is_some_and(ThreadInputState::user_turn_pending_start);
        let mut queued_completion_wakeups = input_state
            .as_mut()
            .map(ThreadInputState::take_queued_completion_wakeup_prompts)
            .unwrap_or_default();
        let mut wakeup_ops = Vec::new();
        if let Some(channel) = self.thread_event_channels.get_mut(&active_id) {
            let receiver = self.active_thread_rx.take();
            let mut store = channel.store.lock().await;
            if store.is_closed() {
                if let Some(input_state) = input_state.as_mut() {
                    input_state.clear_user_turn_pending_start_for_restore();
                    input_state.clear_background_terminal_processes();
                    let _ = input_state.take_queued_completion_wakeup_prompts();
                }
                queued_completion_wakeups.clear();
            }
            store.active = false;
            store.input_state = input_state;
            let session = store.session.clone();
            for text in queued_completion_wakeups {
                if turn_start_pending {
                    store.queue_offscreen_wakeup(text);
                    continue;
                }
                match (
                    store.queue_or_start_offscreen_wakeup(text),
                    session.as_ref(),
                ) {
                    (Some(text), Some(session)) => {
                        wakeup_ops.push(Self::plain_text_wakeup_command(session, text));
                    }
                    (Some(text), None) => {
                        store.cancel_offscreen_wakeup_start();
                        if let Some(input_state) = store.input_state.as_mut() {
                            input_state
                                .queue_background_terminal_completion_prompt_for_restore(text);
                        }
                    }
                    (None, _) => {}
                }
            }
            if let Some(receiver) = receiver {
                channel.receiver = Some(receiver);
            }
        }
        for op in wakeup_ops {
            self.app_event_tx.send(AppEvent::SubmitThreadOp {
                thread_id: active_id,
                op,
            });
        }
    }

    pub(super) async fn activate_thread_for_replay(
        &mut self,
        thread_id: ThreadId,
    ) -> Option<(mpsc::Receiver<ThreadBufferedEvent>, ThreadEventSnapshot)> {
        let channel = self.thread_event_channels.get_mut(&thread_id)?;
        let receiver = channel.receiver.take()?;
        let mut store = channel.store.lock().await;
        store.active = true;
        let snapshot = store.snapshot_for_activation();
        Some((receiver, snapshot))
    }

    pub(super) async fn clear_active_thread(&mut self) {
        if let Some(active_id) = self.active_thread_id.take() {
            self.set_thread_active(active_id, /*active*/ false).await;
        }
        self.active_thread_rx = None;
        self.refresh_pending_thread_approvals().await;
    }

    pub(super) async fn note_thread_outbound_op(&mut self, thread_id: ThreadId, op: &AppCommand) {
        let Some(channel) = self.thread_event_channels.get(&thread_id) else {
            return;
        };
        let mut store = channel.store.lock().await;
        store.note_outbound_op(op);
    }

    pub(super) async fn note_active_thread_outbound_op(&mut self, op: &AppCommand) {
        if !ThreadEventStore::op_can_change_pending_replay_state(op) {
            return;
        }
        let Some(thread_id) = self.active_thread_id else {
            return;
        };
        self.note_thread_outbound_op(thread_id, op).await;
    }

    pub(super) async fn active_turn_id_for_thread(&self, thread_id: ThreadId) -> Option<String> {
        let channel = self.thread_event_channels.get(&thread_id)?;
        let store = channel.store.lock().await;
        store.active_turn_id().map(ToOwned::to_owned)
    }

    pub(super) fn thread_label(&self, thread_id: ThreadId) -> String {
        let is_primary = self.primary_thread_id == Some(thread_id);
        let fallback_label = if is_primary {
            "Main [default]".to_string()
        } else {
            let thread_id = thread_id.to_string();
            let short_id: String = thread_id.chars().take(8).collect();
            format!("Agent ({short_id})")
        };
        if let Some(entry) = self.agent_navigation.get(&thread_id) {
            let label = format_agent_picker_item_name(
                entry.agent_nickname.as_deref(),
                entry.agent_role.as_deref(),
                is_primary,
            );
            if label == "Agent" {
                let thread_id = thread_id.to_string();
                let short_id: String = thread_id.chars().take(8).collect();
                format!("{label} ({short_id})")
            } else {
                label
            }
        } else {
            fallback_label
        }
    }

    /// Returns the thread whose transcript is currently on screen.
    ///
    /// `active_thread_id` is the source of truth during steady state, but the widget can briefly
    /// lag behind thread bookkeeping during transitions. The footer label and adjacent-thread
    /// navigation both follow what the user is actually looking at, not whichever thread most
    /// recently began switching.
    pub(super) fn current_displayed_thread_id(&self) -> Option<ThreadId> {
        self.active_thread_id.or(self.chat_widget.thread_id())
    }

    pub(super) fn ignore_same_thread_resume(
        &mut self,
        target_session: &crate::resume_picker::SessionTarget,
    ) -> bool {
        if self.active_thread_id != Some(target_session.thread_id) {
            return false;
        };

        self.chat_widget.add_info_message(
            format!("Already viewing {}.", target_session.display_label()),
            /*hint*/ None,
        );
        true
    }

    /// Mirrors the visible thread into the contextual footer row.
    ///
    /// The footer sometimes shows ambient context instead of an instructional hint. In multi-agent
    /// sessions, that contextual row includes the currently viewed agent label. The label is
    /// intentionally hidden until there is more than one known thread so single-thread sessions do
    /// not spend footer space restating that the user is already on the main conversation.
    pub(super) fn sync_active_agent_label(&mut self) {
        let label = self
            .agent_navigation
            .active_agent_label(self.current_displayed_thread_id(), self.primary_thread_id);
        self.chat_widget.set_active_agent_label(label);
        let active_subagent_count = self
            .agent_navigation
            .ordered_threads()
            .into_iter()
            .filter(|(thread_id, entry)| {
                Some(*thread_id) != self.primary_thread_id
                    && !entry.is_closed
                    && !self.side_threads.contains_key(thread_id)
            })
            .count();
        self.chat_widget
            .set_active_subagent_count(active_subagent_count);
        let subagent_summaries = self
            .agent_navigation
            .ordered_threads()
            .into_iter()
            .filter(|(thread_id, _)| {
                Some(*thread_id) != self.primary_thread_id
                    && !self.side_threads.contains_key(thread_id)
            })
            .map(|(thread_id, entry)| SubagentActivitySummary {
                thread_id,
                label: format_agent_picker_item_name(
                    entry.agent_nickname.as_deref(),
                    entry.agent_role.as_deref(),
                    /*is_primary*/ false,
                ),
                is_closed: entry.is_closed,
                started_at: self
                    .agent_navigation
                    .started_at(thread_id)
                    .unwrap_or_else(Instant::now),
            })
            .collect();
        self.chat_widget
            .set_subagent_activity_summaries(subagent_summaries);
        self.sync_side_thread_ui();
    }

    fn subagent_activity_label(&self, thread_id: ThreadId) -> String {
        self.agent_navigation
            .get(&thread_id)
            .map(|entry| {
                format_agent_picker_item_name(
                    entry.agent_nickname.as_deref(),
                    entry.agent_role.as_deref(),
                    /*is_primary*/ false,
                )
            })
            .unwrap_or_else(|| {
                format_agent_picker_item_name(
                    /*agent_nickname*/ None, /*agent_role*/ None,
                    /*is_primary*/ false,
                )
            })
    }

    fn should_wake_primary_for_closed_subagent(&self, thread_id: ThreadId) -> bool {
        Some(thread_id) != self.primary_thread_id
            && !self.side_threads.contains_key(&thread_id)
            && self.agent_navigation.get(&thread_id).is_some()
    }

    pub(super) async fn mark_subagent_closed_and_wake_primary_once(&mut self, thread_id: ThreadId) {
        let changed = self.mark_agent_picker_thread_closed(thread_id);
        self.wake_primary_for_closed_subagent_transition(thread_id, changed)
            .await;
    }

    pub(super) async fn wake_primary_for_closed_subagent_transition(
        &mut self,
        thread_id: ThreadId,
        changed: bool,
    ) {
        if changed && self.should_wake_primary_for_closed_subagent(thread_id) {
            let label = self.subagent_activity_label(thread_id);
            self.queue_or_submit_subagent_completion_wakeup(label, thread_id)
                .await;
        }
    }

    pub(super) async fn mark_thread_store_closed_for_liveness(&mut self, thread_id: ThreadId) {
        if let Some(channel) = self.thread_event_channels.get(&thread_id) {
            channel.store.lock().await.mark_closed_for_liveness();
        }
    }

    async fn queue_or_submit_subagent_completion_wakeup(
        &mut self,
        label: String,
        thread_id: ThreadId,
    ) {
        if self.current_displayed_thread_id() == self.primary_thread_id {
            self.chat_widget
                .submit_subagent_completion_wakeup(label, thread_id);
            return;
        }

        if let (Some(primary_thread_id), Some(primary_session)) = (
            self.primary_thread_id,
            self.primary_session_configured.clone(),
        ) {
            let text = Self::subagent_completion_wakeup_prompt(&label, thread_id);
            let op = {
                let store = Arc::clone(&self.ensure_thread_channel(primary_thread_id).store);
                let mut store = store.lock().await;
                store
                    .queue_or_start_offscreen_wakeup(text)
                    .map(|text| Self::plain_text_wakeup_command(&primary_session, text))
            };
            if let Some(op) = op {
                self.app_event_tx.send(AppEvent::SubmitThreadOp {
                    thread_id: primary_thread_id,
                    op,
                });
            }
            return;
        }

        if self
            .pending_subagent_completion_wakeups
            .iter()
            .any(|pending| pending.thread_id == thread_id)
        {
            return;
        }
        self.pending_subagent_completion_wakeups
            .push_back(PendingSubagentCompletionWakeup { thread_id, label });
    }

    pub(super) fn flush_pending_subagent_completion_wakeups(&mut self) {
        if self.current_displayed_thread_id() != self.primary_thread_id {
            return;
        }
        while let Some(pending) = self.pending_subagent_completion_wakeups.pop_front() {
            self.chat_widget
                .submit_subagent_completion_wakeup(pending.label, pending.thread_id);
        }
    }

    pub(super) async fn thread_cwd(&self, thread_id: ThreadId) -> Option<AbsolutePathBuf> {
        let channel = self.thread_event_channels.get(&thread_id)?;
        let store = channel.store.lock().await;
        store.session.as_ref().map(|session| session.cwd.clone())
    }

    async fn thread_file_change_changes(
        &self,
        thread_id: ThreadId,
        turn_id: &str,
        item_id: &str,
    ) -> Option<Vec<codex_app_server_protocol::FileUpdateChange>> {
        let channel = self.thread_event_channels.get(&thread_id)?;
        let store = channel.store.lock().await;
        store.file_change_changes(turn_id, item_id)
    }

    pub(super) async fn interactive_request_for_thread_request(
        &self,
        thread_id: ThreadId,
        request: &ServerRequest,
    ) -> Option<ThreadInteractiveRequest> {
        let thread_label = Some(self.thread_label(thread_id));
        match request {
            ServerRequest::CommandExecutionRequestApproval { params, .. } => {
                let network_approval_context = params.network_approval_context.clone();
                let additional_permissions = params.additional_permissions.clone();
                let proposed_execpolicy_amendment = params.proposed_execpolicy_amendment.clone();
                let proposed_network_policy_amendments =
                    params.proposed_network_policy_amendments.clone();
                Some(ThreadInteractiveRequest::Approval(ApprovalRequest::Exec {
                    thread_id,
                    thread_label,
                    id: params
                        .approval_id
                        .clone()
                        .unwrap_or_else(|| params.item_id.clone()),
                    command: params
                        .command
                        .as_deref()
                        .map(split_command_string)
                        .unwrap_or_default(),
                    reason: params.reason.clone(),
                    available_decisions: params.available_decisions.clone().unwrap_or_else(|| {
                        default_exec_approval_decisions(
                            network_approval_context.as_ref(),
                            proposed_execpolicy_amendment.as_ref(),
                            proposed_network_policy_amendments.as_deref(),
                            additional_permissions.as_ref(),
                        )
                    }),
                    network_approval_context,
                    additional_permissions,
                }))
            }
            ServerRequest::FileChangeRequestApproval { params, .. } => Some(
                ThreadInteractiveRequest::Approval(ApprovalRequest::ApplyPatch {
                    thread_id,
                    thread_label,
                    id: params.item_id.clone(),
                    reason: params.reason.clone(),
                    cwd: self
                        .thread_cwd(thread_id)
                        .await
                        .unwrap_or_else(|| self.config.cwd.clone()),
                    changes: self
                        .thread_file_change_changes(thread_id, &params.turn_id, &params.item_id)
                        .await
                        .map(crate::app_server_approval_conversions::file_update_changes_to_display)
                        .unwrap_or_default(),
                }),
            ),
            ServerRequest::McpServerElicitationRequest { request_id, params } => {
                if let Some(params) = AppLinkViewParams::from_url_app_server_request(
                    thread_id,
                    &params.server_name,
                    request_id.clone(),
                    &params.request,
                ) {
                    Some(ThreadInteractiveRequest::AppLink(params))
                } else if let Some(request) =
                    McpServerElicitationFormRequest::from_app_server_request(
                        thread_id,
                        request_id.clone(),
                        params.clone(),
                    )
                {
                    Some(ThreadInteractiveRequest::McpServerElicitation(request))
                } else {
                    match &params.request {
                        codex_app_server_protocol::McpServerElicitationRequest::Form {
                            message,
                            ..
                        } => Some(ThreadInteractiveRequest::Approval(
                            ApprovalRequest::McpElicitation {
                                thread_id,
                                thread_label,
                                server_name: params.server_name.clone(),
                                request_id: request_id.clone(),
                                message: message.clone(),
                            },
                        )),
                        codex_app_server_protocol::McpServerElicitationRequest::Url { .. } => {
                            self.app_event_tx.resolve_elicitation(
                                thread_id,
                                params.server_name.clone(),
                                request_id.clone(),
                                codex_app_server_protocol::McpServerElicitationAction::Decline,
                                /*content*/ None,
                                /*meta*/ None,
                            );
                            None
                        }
                    }
                }
            }
            ServerRequest::PermissionsRequestApproval { params, .. } => Some(
                ThreadInteractiveRequest::Approval(ApprovalRequest::Permissions {
                    thread_id,
                    thread_label,
                    call_id: params.item_id.clone(),
                    reason: params.reason.clone(),
                    permissions: params.permissions.clone().into(),
                }),
            ),
            _ => None,
        }
    }

    pub(super) fn push_thread_interactive_request(&mut self, request: ThreadInteractiveRequest) {
        match request {
            ThreadInteractiveRequest::AppLink(params) => {
                self.chat_widget.open_app_link_view(params);
            }
            ThreadInteractiveRequest::Approval(request) => {
                self.render_inactive_patch_preview(&request);
                self.chat_widget.push_approval_request(request);
            }
            ThreadInteractiveRequest::McpServerElicitation(request) => {
                self.chat_widget
                    .push_mcp_server_elicitation_request(request);
            }
        }
    }

    fn render_inactive_patch_preview(&mut self, request: &ApprovalRequest) {
        let ApprovalRequest::ApplyPatch {
            thread_label,
            cwd,
            changes,
            ..
        } = request
        else {
            return;
        };
        if thread_label.is_none() || changes.is_empty() {
            return;
        }
        self.chat_widget
            .add_to_history(history_cell::new_patch_event(changes.clone(), cwd));
    }

    pub(super) async fn pending_inactive_thread_requests(&self) -> Vec<(ThreadId, ServerRequest)> {
        let channels: Vec<(ThreadId, Arc<Mutex<ThreadEventStore>>)> = self
            .thread_event_channels
            .iter()
            .map(|(thread_id, channel)| (*thread_id, Arc::clone(&channel.store)))
            .collect();

        let mut requests = Vec::new();
        for (thread_id, store) in channels {
            if Some(thread_id) == self.active_thread_id {
                continue;
            }

            let store = store.lock().await;
            requests.extend(
                store
                    .pending_replay_requests()
                    .into_iter()
                    .map(|request| (thread_id, request)),
            );
        }
        requests
    }

    pub(super) async fn surface_pending_inactive_thread_interactive_requests(&mut self) {
        if self.active_side_parent_thread_id().is_some() {
            return;
        }

        let requests = self.pending_inactive_thread_requests().await;
        for (thread_id, request) in requests {
            if let Some(request) = self
                .interactive_request_for_thread_request(thread_id, &request)
                .await
            {
                self.push_thread_interactive_request(request);
            }
        }
    }

    pub(super) async fn submit_active_thread_op(
        &mut self,
        app_server: &mut AppServerSession,
        op: AppCommand,
    ) -> Result<()> {
        let Some(thread_id) = self.active_thread_id else {
            self.chat_widget
                .add_error_message("No active thread is available.".to_string());
            return Ok(());
        };

        self.submit_thread_op(app_server, thread_id, op).await
    }

    pub(super) async fn submit_thread_op(
        &mut self,
        app_server: &mut AppServerSession,
        thread_id: ThreadId,
        op: AppCommand,
    ) -> Result<()> {
        crate::session_log::log_outbound_op(&op);

        if self
            .try_resolve_app_server_request(app_server, thread_id, &op)
            .await?
        {
            return Ok(());
        }

        if self.thread_is_closed_or_marked_closed(thread_id).await {
            tracing::warn!(thread_id = %thread_id, "dropping op for closed thread");
            return Ok(());
        }

        if self
            .try_submit_active_thread_op_via_app_server(app_server, thread_id, &op)
            .await?
        {
            if ThreadEventStore::op_can_change_pending_replay_state(&op) {
                self.note_thread_outbound_op(thread_id, &op).await;
                self.refresh_pending_thread_approvals().await;
                self.refresh_side_parent_status_from_store(thread_id).await;
            }
            return Ok(());
        }

        self.chat_widget
            .add_error_message(format!("Not available in TUI yet for thread {thread_id}."));
        Ok(())
    }

    /// Persist prompt text in the local cross-session message history.
    pub(super) fn append_message_history_entry(&self, thread_id: ThreadId, text: String) {
        let history_config = codex_message_history::HistoryConfig::new(
            self.chat_widget.config_ref().codex_home.clone(),
            &self.chat_widget.config_ref().history,
        );
        tokio::spawn(async move {
            if let Err(err) =
                codex_message_history::append_entry(&text, thread_id, &history_config).await
            {
                tracing::warn!(
                    thread_id = %thread_id,
                    error = %err,
                    "failed to append to message history"
                );
            }
        });
    }

    /// Fetch one local cross-session message history entry for the requesting thread.
    pub(super) async fn lookup_message_history_entry(
        &mut self,
        thread_id: ThreadId,
        offset: usize,
        log_id: u64,
    ) -> Result<()> {
        let history_config = codex_message_history::HistoryConfig::new(
            self.chat_widget.config_ref().codex_home.clone(),
            &self.chat_widget.config_ref().history,
        );
        let app_event_tx = self.app_event_tx.clone();
        tokio::spawn(async move {
            let entry_opt = tokio::task::spawn_blocking(move || {
                codex_message_history::lookup(log_id, offset, &history_config)
            })
            .await
            .unwrap_or_else(|err| {
                tracing::warn!(error = %err, "history lookup task failed");
                None
            });

            app_event_tx.send(AppEvent::ThreadHistoryEntryResponse {
                thread_id,
                event: HistoryLookupResponse {
                    offset,
                    log_id,
                    entry: entry_opt.map(|entry| entry.text),
                },
            });
        });
        Ok(())
    }

    pub(super) async fn try_submit_active_thread_op_via_app_server(
        &mut self,
        app_server: &mut AppServerSession,
        thread_id: ThreadId,
        op: &AppCommand,
    ) -> Result<bool> {
        match op {
            AppCommand::Interrupt => {
                if let Some(turn_id) = self.active_turn_id_for_thread(thread_id).await {
                    app_server.turn_interrupt(thread_id, turn_id).await?;
                } else {
                    app_server.startup_interrupt(thread_id).await?;
                }
                Ok(true)
            }
            AppCommand::UserTurn {
                items,
                cwd,
                approval_policy,
                approvals_reviewer,
                permission_profile,
                model,
                effort,
                summary,
                service_tier,
                final_output_json_schema,
                collaboration_mode,
                personality,
            } => {
                let mut should_start_turn = true;
                if let Some(turn_id) = self.active_turn_id_for_thread(thread_id).await {
                    let mut steer_turn_id = turn_id;
                    let mut retried_after_turn_mismatch = false;
                    loop {
                        match app_server
                            .turn_steer(thread_id, steer_turn_id.clone(), items.to_vec())
                            .await
                        {
                            Ok(_) => return Ok(true),
                            Err(error) => {
                                if let Some(turn_error) =
                                    active_turn_not_steerable_turn_error(&error)
                                {
                                    if !self.chat_widget.enqueue_rejected_steer() {
                                        self.chat_widget.add_error_message(turn_error.message);
                                    }
                                    return Ok(true);
                                }
                                match active_turn_steer_race(&error) {
                                    Some(ActiveTurnSteerRace::Missing) => {
                                        if let Some(channel) =
                                            self.thread_event_channels.get(&thread_id)
                                        {
                                            let mut store = channel.store.lock().await;
                                            store.clear_active_turn_id();
                                        }
                                        should_start_turn = true;
                                        break;
                                    }
                                    Some(ActiveTurnSteerRace::ExpectedTurnMismatch {
                                        actual_turn_id,
                                    }) if !retried_after_turn_mismatch
                                        && actual_turn_id != steer_turn_id =>
                                    {
                                        // Review flows can swap the active turn before the TUI
                                        // processes the corresponding notification. Retry once with
                                        // the server-reported turn id so non-steerable review turns
                                        // still fall through to the existing queueing behavior.
                                        if let Some(channel) =
                                            self.thread_event_channels.get(&thread_id)
                                        {
                                            let mut store = channel.store.lock().await;
                                            store.active_turn_id = Some(actual_turn_id.clone());
                                        }
                                        steer_turn_id = actual_turn_id;
                                        retried_after_turn_mismatch = true;
                                    }
                                    Some(ActiveTurnSteerRace::ExpectedTurnMismatch {
                                        actual_turn_id,
                                    }) => {
                                        if let Some(channel) =
                                            self.thread_event_channels.get(&thread_id)
                                        {
                                            let mut store = channel.store.lock().await;
                                            store.active_turn_id = Some(actual_turn_id);
                                        }
                                        return Err(error.into());
                                    }
                                    None => return Err(error.into()),
                                }
                            }
                        }
                    }
                }
                if should_start_turn {
                    let config = self.chat_widget.config_ref();
                    let approvals_reviewer =
                        approvals_reviewer.unwrap_or(config.approvals_reviewer);
                    let active_permission_profile =
                        if config.permissions.permission_profile() == permission_profile.clone() {
                            config.permissions.active_permission_profile()
                        } else {
                            None
                        };
                    app_server
                        .turn_start(
                            thread_id,
                            items.to_vec(),
                            cwd.clone(),
                            *approval_policy,
                            approvals_reviewer,
                            permission_profile.clone(),
                            active_permission_profile,
                            model.to_string(),
                            *effort,
                            *summary,
                            service_tier.clone(),
                            collaboration_mode.clone(),
                            *personality,
                            final_output_json_schema.clone(),
                        )
                        .await?;
                }
                Ok(true)
            }
            AppCommand::ListSkills { cwds, force_reload } => {
                self.handle_skills_list_result(
                    app_server
                        .skills_list(codex_app_server_protocol::SkillsListParams {
                            cwds: cwds.clone(),
                            force_reload: *force_reload,
                        })
                        .await,
                    "failed to refresh skills",
                );
                Ok(true)
            }
            AppCommand::Compact => {
                app_server.thread_compact_start(thread_id).await?;
                Ok(true)
            }
            AppCommand::SetThreadName { name } => {
                app_server
                    .thread_set_name(thread_id, name.to_string())
                    .await?;
                Ok(true)
            }
            AppCommand::ThreadRollback { num_turns } => {
                let response = match app_server.thread_rollback(thread_id, *num_turns).await {
                    Ok(response) => response,
                    Err(err) => {
                        self.handle_backtrack_rollback_failed();
                        return Err(err);
                    }
                };
                self.handle_thread_rollback_response(thread_id, *num_turns, &response)
                    .await;
                Ok(true)
            }
            AppCommand::Review { target } => {
                app_server.review_start(thread_id, target.clone()).await?;
                Ok(true)
            }
            AppCommand::CleanBackgroundTerminals => {
                app_server
                    .thread_background_terminals_clean(thread_id)
                    .await?;
                self.mark_background_terminals_stopped_for_thread(thread_id)
                    .await;
                if self.active_thread_id == Some(thread_id) {
                    self.chat_widget.clear_background_terminals_after_clean();
                }
                Ok(true)
            }
            AppCommand::TerminateBackgroundTerminal { process_id } => {
                app_server
                    .thread_background_terminal_terminate(thread_id, process_id.to_string())
                    .await?;
                Ok(true)
            }
            AppCommand::RealtimeConversationStart { transport, voice } => {
                app_server
                    .thread_realtime_start(thread_id, transport.clone(), voice.clone())
                    .await?;
                Ok(true)
            }
            AppCommand::RealtimeConversationAudio(frame) => {
                app_server
                    .thread_realtime_audio(thread_id, frame.clone())
                    .await?;
                Ok(true)
            }
            AppCommand::RealtimeConversationClose => {
                app_server.thread_realtime_stop(thread_id).await?;
                Ok(true)
            }
            AppCommand::RunUserShellCommand { command } => {
                app_server
                    .thread_shell_command(thread_id, command.to_string())
                    .await?;
                Ok(true)
            }
            AppCommand::ReloadUserConfig => {
                app_server.reload_user_config().await?;
                let discovery_cwd = self.chat_widget.discovery_cwd().to_path_buf();
                self.refresh_in_memory_config_from_disk_for_discovery_cwd(discovery_cwd)
                    .await?;
                Ok(true)
            }
            AppCommand::OverrideTurnContext { cwd, .. } => {
                if let Some(cwd) = cwd {
                    app_server
                        .thread_runtime_cwd_update(thread_id, cwd.clone())
                        .await?;
                }
                Ok(true)
            }
            AppCommand::ApproveGuardianDeniedAction { event } => {
                app_server
                    .thread_approve_guardian_denied_action(thread_id, event)
                    .await?;
                Ok(true)
            }
            _ => Ok(false),
        }
    }

    pub(super) fn handle_skills_list_result(
        &mut self,
        result: Result<SkillsListResponse>,
        failure_message: &str,
    ) {
        match result {
            Ok(response) => self.handle_skills_list_response(response),
            Err(err) => {
                tracing::warn!("{failure_message}: {err:#}");
                self.chat_widget
                    .add_error_message(format!("{failure_message}: {err:#}"));
            }
        }
    }

    pub(super) async fn try_resolve_app_server_request(
        &mut self,
        app_server: &AppServerSession,
        thread_id: ThreadId,
        op: &AppCommand,
    ) -> Result<bool> {
        let Some(resolution) = self
            .pending_app_server_requests
            .take_resolution(thread_id, op)
            .map_err(|err| color_eyre::eyre::eyre!(err))?
        else {
            return Ok(false);
        };

        match app_server
            .resolve_server_request(resolution.request_id, resolution.result)
            .await
        {
            Ok(()) => {
                if ThreadEventStore::op_can_change_pending_replay_state(op) {
                    self.note_thread_outbound_op(thread_id, op).await;
                    self.refresh_pending_thread_approvals().await;
                    self.refresh_side_parent_status_from_store(thread_id).await;
                }
                Ok(true)
            }
            Err(err) => {
                self.chat_widget.add_error_message(format!(
                    "Failed to resolve app-server request for thread {thread_id}: {err}"
                ));
                Ok(false)
            }
        }
    }

    pub(super) async fn refresh_pending_thread_approvals(&mut self) {
        let side_parent_thread_id = self.active_side_parent_thread_id();
        let channels: Vec<(ThreadId, Arc<Mutex<ThreadEventStore>>)> = self
            .thread_event_channels
            .iter()
            .map(|(thread_id, channel)| (*thread_id, Arc::clone(&channel.store)))
            .collect();

        let mut pending_thread_ids = Vec::new();
        for (thread_id, store) in channels {
            if Some(thread_id) == self.active_thread_id || Some(thread_id) == side_parent_thread_id
            {
                continue;
            }

            let store = store.lock().await;
            if store.has_pending_thread_approvals() {
                pending_thread_ids.push(thread_id);
            }
        }

        pending_thread_ids.sort_by_key(ThreadId::to_string);

        let threads = pending_thread_ids
            .into_iter()
            .map(|thread_id| self.thread_label(thread_id))
            .collect();

        self.chat_widget.set_pending_thread_approvals(threads);
    }

    pub(super) async fn refresh_side_parent_status_from_store(&mut self, thread_id: ThreadId) {
        let Some(channel) = self.thread_event_channels.get(&thread_id) else {
            return;
        };
        let status = {
            let store = channel.store.lock().await;
            store.side_parent_pending_status()
        };
        if let Some(status) = status {
            self.set_side_parent_status(thread_id, Some(status));
        } else {
            self.clear_side_parent_action_status(thread_id);
        }
    }

    pub(super) async fn enqueue_thread_notification(
        &mut self,
        thread_id: ThreadId,
        notification: ServerNotification,
    ) -> Result<()> {
        if self.discarded_side_thread_ids.contains(&thread_id) {
            tracing::debug!(
                thread_id = %thread_id,
                "ignoring notification for discarded side thread"
            );
            return Ok(());
        }
        if matches!(notification, ServerNotification::ThreadClosed(_))
            && self.primary_thread_id != Some(thread_id)
            && !self.thread_event_channels.contains_key(&thread_id)
            && self.agent_navigation.get(&thread_id).is_none()
            && !self.side_threads.contains_key(&thread_id)
        {
            tracing::debug!(
                thread_id = %thread_id,
                "ignoring ThreadClosed for unknown inactive thread"
            );
            return Ok(());
        }

        let inferred_session = self
            .infer_session_for_thread_notification(thread_id, &notification)
            .await;
        let (sender, store) = {
            let channel = self.ensure_thread_channel(thread_id);
            (channel.sender.clone(), Arc::clone(&channel.store))
        };

        let (should_send, pending_status, wakeup_op, drop_notification) = {
            let mut guard = store.lock().await;
            if guard.session.is_none()
                && let Some(session) = inferred_session
            {
                guard.session = Some(session);
            }
            let inactive_terminal_result =
                guard.track_inactive_background_terminal_notification(&notification);
            if !inactive_terminal_result.drop_notification {
                guard.push_notification(notification.clone());
            }
            let wakeup_text = inactive_terminal_result
                .wakeup_text
                .or_else(|| guard.take_ready_offscreen_wakeup());
            let wakeup_op = match (wakeup_text, guard.session.clone()) {
                (Some(text), Some(session)) => {
                    Some(Self::plain_text_wakeup_command(&session, text))
                }
                (Some(text), None) => {
                    guard.cancel_offscreen_wakeup_start();
                    if let Some(input_state) = guard.input_state.as_mut() {
                        input_state.queue_background_terminal_completion_prompt_for_restore(text);
                    }
                    None
                }
                (None, _) => None,
            };
            (
                guard.active && !inactive_terminal_result.drop_notification,
                guard.side_parent_pending_status(),
                wakeup_op,
                inactive_terminal_result.drop_notification,
            )
        };
        let notification_status_change = (!drop_notification)
            .then(|| SideParentStatusChange::for_notification(&notification))
            .flatten();
        let is_thread_closed = matches!(&notification, ServerNotification::ThreadClosed(_));

        if should_send {
            match sender.try_send(ThreadBufferedEvent::Notification(notification.clone())) {
                Ok(()) => {}
                Err(TrySendError::Full(event)) => {
                    tokio::spawn(async move {
                        if let Err(err) = sender.send(event).await {
                            tracing::warn!("thread {thread_id} event channel closed: {err}");
                        }
                    });
                }
                Err(TrySendError::Closed(_)) => {
                    tracing::warn!("thread {thread_id} event channel closed");
                }
            }
        }
        if let Some(status) = pending_status {
            self.set_side_parent_status(thread_id, Some(status));
        } else if let Some(change) = notification_status_change {
            self.apply_side_parent_status_change(thread_id, change);
        }
        if is_thread_closed
            && self.active_thread_id != Some(thread_id)
            && self.should_wake_primary_for_closed_subagent(thread_id)
        {
            self.mark_subagent_closed_and_wake_primary_once(thread_id)
                .await;
        }
        if let Some(op) = wakeup_op {
            self.app_event_tx
                .send(AppEvent::SubmitThreadOp { thread_id, op });
        }
        self.sync_background_terminal_activity_summaries().await;
        self.refresh_pending_thread_approvals().await;
        Ok(())
    }

    /// Eagerly fetches nickname and role for receiver threads referenced by a collab notification.
    ///
    /// This runs on every buffered thread notification before it reaches rendering. For each
    /// receiver thread id that the navigation cache does not yet have metadata for, it issues a
    /// `thread/read` RPC and registers the result in both `AgentNavigationState` and the
    /// `ChatWidget` metadata map. Threads that already have a nickname or role cached are skipped,
    /// so the cost is at most one RPC per thread over the lifetime of a session.
    ///
    /// Failures are logged and silently ignored -- the worst outcome is that a rendered item shows
    /// a thread id instead of a human-readable name, which is the same behavior the TUI had before
    /// this change.
    pub(super) async fn hydrate_collab_agent_metadata_for_notification(
        &mut self,
        app_server: &mut AppServerSession,
        notification: &ServerNotification,
    ) {
        let Some(receiver_thread_ids) = collab_receiver_thread_ids(notification) else {
            return;
        };

        for receiver_thread_id in receiver_thread_ids {
            let Ok(thread_id) = ThreadId::from_string(receiver_thread_id) else {
                tracing::warn!(
                    thread_id = receiver_thread_id,
                    "ignoring collab receiver with invalid thread id during metadata hydration"
                );
                continue;
            };

            if self
                .agent_navigation
                .get(&thread_id)
                .is_some_and(|entry| entry.agent_nickname.is_some() || entry.agent_role.is_some())
            {
                continue;
            }

            match app_server
                .thread_read(thread_id, /*include_turns*/ false)
                .await
            {
                Ok(thread) => {
                    let is_closed = matches!(
                        thread.status,
                        codex_app_server_protocol::ThreadStatus::NotLoaded
                    ) || self
                        .agent_navigation
                        .get(&thread_id)
                        .is_some_and(|entry| entry.is_closed);
                    self.upsert_agent_picker_thread(
                        thread_id,
                        thread.agent_nickname,
                        thread.agent_role,
                        is_closed,
                    );
                }
                Err(err) => {
                    tracing::warn!(
                        thread_id = %thread_id,
                        error = %err,
                        "failed to hydrate collab receiver thread metadata"
                    );
                }
            }
        }
    }

    pub(super) async fn infer_session_for_thread_notification(
        &mut self,
        thread_id: ThreadId,
        notification: &ServerNotification,
    ) -> Option<ThreadSessionState> {
        let ServerNotification::ThreadStarted(notification) = notification else {
            return None;
        };
        let mut session = self.primary_session_configured.clone()?;
        session.thread_id = thread_id;
        session.thread_name = notification.thread.name.clone();
        session.model_provider_id = notification.thread.model_provider.clone();
        session.cwd = notification.thread.cwd.clone();
        let rollout_path = notification.thread.path.clone();
        if let Some(model) =
            read_session_model(self.state_db.as_deref(), thread_id, rollout_path.as_deref()).await
        {
            session.model = model;
        } else if rollout_path.is_some() {
            session.model.clear();
        }
        session.message_history = None;
        session.rollout_path = rollout_path;
        self.upsert_agent_picker_thread(
            thread_id,
            notification.thread.agent_nickname.clone(),
            notification.thread.agent_role.clone(),
            /*is_closed*/ false,
        );
        Some(session)
    }

    pub(super) async fn enqueue_thread_request(
        &mut self,
        thread_id: ThreadId,
        request: ServerRequest,
    ) -> Result<ThreadRequestEnqueueResult> {
        if self.discarded_side_thread_ids.contains(&thread_id) {
            return Ok(ThreadRequestEnqueueResult::ThreadClosed);
        }
        if self
            .agent_navigation
            .get(&thread_id)
            .is_some_and(|entry| entry.is_closed)
        {
            return Ok(ThreadRequestEnqueueResult::ThreadClosed);
        }

        let (sender, store) = {
            let channel = self.ensure_thread_channel(thread_id);
            (channel.sender.clone(), Arc::clone(&channel.store))
        };

        let (should_send, pending_status) = {
            let mut guard = store.lock().await;
            if guard.is_closed() {
                return Ok(ThreadRequestEnqueueResult::ThreadClosed);
            }
            guard.push_request(request.clone());
            (guard.active, guard.side_parent_pending_status())
        };
        let inactive_interactive_request = if self.active_thread_id != Some(thread_id) {
            self.interactive_request_for_thread_request(thread_id, &request)
                .await
        } else {
            None
        };
        let request_status = SideParentStatus::for_request(&request);

        if should_send {
            match sender.try_send(ThreadBufferedEvent::Request(request)) {
                Ok(()) => {}
                Err(TrySendError::Full(event)) => {
                    tokio::spawn(async move {
                        if let Err(err) = sender.send(event).await {
                            tracing::warn!("thread {thread_id} event channel closed: {err}");
                        }
                    });
                }
                Err(TrySendError::Closed(_)) => {
                    tracing::warn!("thread {thread_id} event channel closed");
                }
            }
        } else if self.active_side_parent_thread_id().is_none()
            && let Some(request) = inactive_interactive_request
        {
            self.push_thread_interactive_request(request);
        }
        if let Some(status) = pending_status.or(request_status) {
            self.set_side_parent_status(thread_id, Some(status));
        }
        self.refresh_pending_thread_approvals().await;
        Ok(ThreadRequestEnqueueResult::Enqueued)
    }

    pub(super) async fn enqueue_thread_history_entry_response(
        &mut self,
        thread_id: ThreadId,
        event: HistoryLookupResponse,
    ) -> Result<()> {
        if self.thread_is_closed_or_marked_closed(thread_id).await {
            tracing::debug!(
                thread_id = %thread_id,
                "ignoring history entry response for closed or discarded thread"
            );
            return Ok(());
        }

        let (sender, store) = {
            let channel = self.ensure_thread_channel(thread_id);
            (channel.sender.clone(), Arc::clone(&channel.store))
        };

        let should_send = {
            let mut guard = store.lock().await;
            guard
                .buffer
                .push_back(ThreadBufferedEvent::HistoryEntryResponse(event.clone()));
            if guard.buffer.len() > guard.capacity
                && let Some(removed) = guard.buffer.pop_front()
                && let ThreadBufferedEvent::Request(request) = &removed
            {
                guard
                    .pending_interactive_replay
                    .note_evicted_server_request(request);
            }
            guard.active
        };

        if should_send {
            match sender.try_send(ThreadBufferedEvent::HistoryEntryResponse(event)) {
                Ok(()) => {}
                Err(TrySendError::Full(event)) => {
                    tokio::spawn(async move {
                        if let Err(err) = sender.send(event).await {
                            tracing::warn!("thread {thread_id} event channel closed: {err}");
                        }
                    });
                }
                Err(TrySendError::Closed(_)) => {
                    tracing::warn!("thread {thread_id} event channel closed");
                }
            }
        }
        Ok(())
    }

    pub(super) async fn enqueue_primary_thread_session(
        &mut self,
        mut session: ThreadSessionState,
        turns: Vec<Turn>,
        runtime_cwd_override: Option<PathBuf>,
    ) -> Result<()> {
        let thread_id = session.thread_id;
        if runtime_cwd_override.is_some() {
            session.cwd = self.config.cwd.clone();
        }
        self.primary_thread_id = Some(thread_id);
        self.primary_session_configured = Some(session.clone());
        self.upsert_agent_picker_thread(
            thread_id, /*agent_nickname*/ None, /*agent_role*/ None,
            /*is_closed*/ false,
        );
        let channel = self.ensure_thread_channel(thread_id);
        {
            let mut store = channel.store.lock().await;
            store.set_session(session.clone(), turns.clone());
        }
        self.activate_thread_channel(thread_id).await;
        self.chat_widget
            .set_initial_user_message_submit_suppressed(/*suppressed*/ true);
        self.chat_widget.handle_thread_session(session);
        if let Some(cwd) = runtime_cwd_override {
            let cwd = AbsolutePathBuf::relative_to_current_dir(cwd)?;
            self.apply_runtime_cwd_override(thread_id, cwd).await;
        }
        let should_buffer_initial_replay =
            self.terminal_resize_reflow_enabled() && !turns.is_empty();
        if should_buffer_initial_replay {
            self.app_event_tx
                .send(AppEvent::BeginInitialHistoryReplayBuffer);
        }
        self.chat_widget
            .replay_thread_turns(turns, ReplayKind::ResumeInitialMessages);
        if should_buffer_initial_replay {
            self.app_event_tx
                .send(AppEvent::EndInitialHistoryReplayBuffer);
        }
        let pending = std::mem::take(&mut self.pending_primary_events);
        for pending_event in pending {
            match pending_event {
                ThreadBufferedEvent::Notification(notification) => {
                    self.enqueue_thread_notification(thread_id, notification)
                        .await?;
                }
                ThreadBufferedEvent::Request(request) => {
                    self.enqueue_thread_request(thread_id, request).await?;
                }
                ThreadBufferedEvent::HistoryEntryResponse(event) => {
                    self.enqueue_thread_history_entry_response(thread_id, event)
                        .await?;
                }
                ThreadBufferedEvent::FeedbackSubmission(event) => {
                    self.enqueue_thread_feedback_event(thread_id, event).await;
                }
            }
        }
        self.chat_widget
            .set_initial_user_message_submit_suppressed(/*suppressed*/ false);
        self.chat_widget.submit_initial_user_message_if_pending();
        Ok(())
    }

    pub(super) async fn enqueue_primary_thread_notification(
        &mut self,
        notification: ServerNotification,
    ) -> Result<()> {
        if let Some(thread_id) = self.primary_thread_id {
            return self
                .enqueue_thread_notification(thread_id, notification)
                .await;
        }
        self.pending_primary_events
            .push_back(ThreadBufferedEvent::Notification(notification));
        Ok(())
    }

    pub(super) async fn enqueue_primary_thread_request(
        &mut self,
        request: ServerRequest,
    ) -> Result<ThreadRequestEnqueueResult> {
        if let Some(thread_id) = self.primary_thread_id {
            return self.enqueue_thread_request(thread_id, request).await;
        }
        self.pending_primary_events
            .push_back(ThreadBufferedEvent::Request(request));
        Ok(ThreadRequestEnqueueResult::Enqueued)
    }

    pub(super) async fn thread_is_closed_or_marked_closed(&self, thread_id: ThreadId) -> bool {
        if self.discarded_side_thread_ids.contains(&thread_id) {
            return true;
        }
        let marked_closed = self
            .agent_navigation
            .get(&thread_id)
            .is_some_and(|entry| entry.is_closed);
        if let Some(channel) = self.thread_event_channels.get(&thread_id) {
            return marked_closed || channel.store.lock().await.is_closed();
        }
        marked_closed
    }

    pub(super) async fn refresh_snapshot_session_if_needed(
        &mut self,
        app_server: &mut AppServerSession,
        thread_id: ThreadId,
        is_replay_only: bool,
        snapshot: &mut ThreadEventSnapshot,
    ) {
        if !self.should_refresh_snapshot_session(thread_id, is_replay_only, snapshot) {
            return;
        }

        match app_server
            .resume_thread(self.config.clone(), thread_id)
            .await
        {
            Ok(started) => {
                self.apply_refreshed_snapshot_thread(thread_id, started, snapshot)
                    .await
            }
            Err(err) => {
                tracing::warn!(
                    thread_id = %thread_id,
                    error = %err,
                    "failed to refresh inferred thread session before replay"
                );
            }
        }
    }

    pub(super) fn should_refresh_snapshot_session(
        &self,
        thread_id: ThreadId,
        is_replay_only: bool,
        snapshot: &ThreadEventSnapshot,
    ) -> bool {
        !is_replay_only
            && !self.side_threads.contains_key(&thread_id)
            && snapshot.session.as_ref().is_none_or(|session| {
                session.model.trim().is_empty() || session.rollout_path.is_none()
            })
    }

    pub(super) async fn apply_refreshed_snapshot_thread(
        &mut self,
        thread_id: ThreadId,
        started: AppServerStartedThread,
        snapshot: &mut ThreadEventSnapshot,
    ) {
        let AppServerStartedThread { session, turns } = started;
        if let Some(channel) = self.thread_event_channels.get(&thread_id) {
            let mut store = channel.store.lock().await;
            store.set_session(session.clone(), turns.clone());
            store.rebase_buffer_after_session_refresh();
        }
        snapshot.session = Some(session);
        snapshot.turns = turns;
        snapshot
            .events
            .retain(ThreadEventStore::event_survives_session_refresh);
    }

    /// Opens the `/agent` picker after refreshing cached labels for known threads.
    ///
    /// The picker state is derived from long-lived thread channels plus best-effort metadata
    /// refreshes from the backend. Refresh failures are treated as "thread is only inspectable by
    /// historical id now" and converted into closed picker entries instead of deleting them, so
    /// the stable traversal order remains intact for review and keyboard navigation.
    pub(super) async fn drain_active_thread_events(
        &mut self,
        tui: &mut tui::Tui,
        app_server: &mut AppServerSession,
    ) -> Result<()> {
        let Some(drained_thread_id) = self.active_thread_id else {
            return Ok(());
        };
        let Some(mut rx) = self.active_thread_rx.take() else {
            return Ok(());
        };

        let mut disconnected = false;
        let mut active_thread_changed = false;
        loop {
            match rx.try_recv() {
                Ok(event) => {
                    if self
                        .should_drop_stopped_background_terminal_event(&event)
                        .await
                    {
                        continue;
                    }
                    self.handle_active_thread_event(tui, app_server, event)
                        .await?;
                    if self.active_thread_id != Some(drained_thread_id) {
                        active_thread_changed = true;
                        break;
                    }
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    disconnected = true;
                    break;
                }
            }
        }

        if active_thread_changed {
            if !disconnected
                && let Some(channel) = self.thread_event_channels.get_mut(&drained_thread_id)
            {
                channel.receiver = Some(rx);
            }
        } else if !disconnected {
            self.active_thread_rx = Some(rx);
        } else if self.active_thread_id == Some(drained_thread_id) {
            self.clear_active_thread().await;
        }

        if self.backtrack_render_pending {
            tui.frame_requester().schedule_frame();
        }
        Ok(())
    }

    /// Returns `(closed_thread_id, primary_thread_id)` when a non-primary active
    /// thread has died and we should fail over to the primary thread.
    ///
    /// A user-requested shutdown (`ExitMode::ShutdownFirst`) sets
    /// `pending_shutdown_exit_thread_id`; matching shutdown completions are ignored
    /// here so Ctrl+C-like exits don't accidentally resurrect the main thread.
    ///
    /// Failover is only eligible when all of these are true:
    /// 1. the event is `thread/closed`;
    /// 2. the active thread differs from the primary thread;
    /// 3. the active thread is not the pending shutdown-exit thread.
    pub(super) fn active_non_primary_shutdown_target(
        &self,
        notification: &ServerNotification,
    ) -> Option<(ThreadId, ThreadId)> {
        if !matches!(notification, ServerNotification::ThreadClosed(_)) {
            return None;
        }
        let active_thread_id = self.active_thread_id?;
        let primary_thread_id = self.primary_thread_id?;
        if self.pending_shutdown_exit_thread_id == Some(active_thread_id) {
            return None;
        }
        (active_thread_id != primary_thread_id).then_some((active_thread_id, primary_thread_id))
    }

    pub(super) fn replay_thread_snapshot(
        &mut self,
        snapshot: ThreadEventSnapshot,
        resume_restored_queue: bool,
    ) {
        let should_buffer_replay = self.terminal_resize_reflow_enabled()
            && (!snapshot.turns.is_empty() || !snapshot.events.is_empty());
        if should_buffer_replay {
            self.app_event_tx
                .send(AppEvent::BeginThreadSwitchHistoryReplayBuffer);
        }
        let suppress_replay_notices =
            replay_filter::snapshot_has_pending_interactive_request(&snapshot);
        if let Some(session) = snapshot.session {
            if self.side_threads.contains_key(&session.thread_id) {
                self.chat_widget.handle_side_thread_session(session);
            } else if suppress_replay_notices {
                self.chat_widget.handle_thread_session_quiet(session);
            } else {
                self.chat_widget.handle_thread_session(session);
            }
        }
        self.chat_widget
            .set_queue_autosend_suppressed(/*suppressed*/ true);
        self.chat_widget
            .restore_thread_input_state(snapshot.input_state);
        if !snapshot.turns.is_empty() {
            self.chat_widget
                .replay_thread_turns(snapshot.turns, ReplayKind::ThreadSnapshot);
        }
        for event in snapshot.events {
            if suppress_replay_notices && replay_filter::event_is_notice(&event) {
                continue;
            }
            self.handle_thread_event_replay(event);
        }
        self.chat_widget.mark_replayed_background_terminals_live();
        if should_buffer_replay {
            self.app_event_tx
                .send(AppEvent::EndInitialHistoryReplayBuffer);
        }
        self.chat_widget
            .set_queue_autosend_suppressed(/*suppressed*/ false);
        self.chat_widget
            .set_initial_user_message_submit_suppressed(/*suppressed*/ false);
        self.chat_widget.submit_initial_user_message_if_pending();
        if resume_restored_queue {
            self.chat_widget.maybe_send_next_queued_input();
        }
        self.refresh_status_line();
    }

    pub(super) fn should_wait_for_initial_session(session_selection: &SessionSelection) -> bool {
        matches!(
            session_selection,
            SessionSelection::StartFresh | SessionSelection::Exit
        )
    }

    pub(super) fn should_prompt_for_paused_goal_after_startup_resume(
        session_selection: &SessionSelection,
        initial_prompt: &Option<String>,
        initial_images: &[PathBuf],
    ) -> bool {
        matches!(session_selection, SessionSelection::Resume(_))
            && initial_prompt.is_none()
            && initial_images.is_empty()
    }

    pub(super) fn should_handle_active_thread_events(
        waiting_for_initial_session_configured: bool,
        has_active_thread_receiver: bool,
    ) -> bool {
        has_active_thread_receiver && !waiting_for_initial_session_configured
    }

    pub(super) fn should_stop_waiting_for_initial_session(
        waiting_for_initial_session_configured: bool,
        primary_thread_id: Option<ThreadId>,
    ) -> bool {
        waiting_for_initial_session_configured && primary_thread_id.is_some()
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) fn handle_skills_list_response(&mut self, response: SkillsListResponse) {
        let cwd = self.chat_widget.discovery_cwd().clone();
        let errors = errors_for_cwd(&cwd, &response);
        emit_skill_load_warnings(&self.app_event_tx, &errors);
        self.chat_widget.handle_skills_list_response(response);
    }

    pub(super) async fn handle_thread_rollback_response(
        &mut self,
        thread_id: ThreadId,
        num_turns: u32,
        response: &ThreadRollbackResponse,
    ) {
        if let Some(channel) = self.thread_event_channels.get(&thread_id) {
            let mut store = channel.store.lock().await;
            store.apply_thread_rollback(response);
        }
        if self.active_thread_id == Some(thread_id)
            && let Some(mut rx) = self.active_thread_rx.take()
        {
            let mut disconnected = false;
            loop {
                match rx.try_recv() {
                    Ok(_) => {}
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => {
                        disconnected = true;
                        break;
                    }
                }
            }

            if !disconnected {
                self.active_thread_rx = Some(rx);
            } else {
                self.clear_active_thread().await;
            }
        }
        self.handle_backtrack_rollback_succeeded(num_turns);
    }

    pub(super) fn handle_thread_event_now(&mut self, event: ThreadBufferedEvent) {
        let needs_refresh = matches!(
            &event,
            ThreadBufferedEvent::Notification(ServerNotification::TurnStarted(_))
                | ThreadBufferedEvent::Notification(ServerNotification::ThreadTokenUsageUpdated(_))
        );
        match event {
            ThreadBufferedEvent::Notification(notification) => {
                self.chat_widget
                    .handle_server_notification(notification, /*replay_kind*/ None);
            }
            ThreadBufferedEvent::Request(request) => {
                if self
                    .pending_app_server_requests
                    .contains_server_request(&request)
                {
                    self.chat_widget
                        .handle_server_request(request, /*replay_kind*/ None);
                }
            }
            ThreadBufferedEvent::HistoryEntryResponse(event) => {
                self.chat_widget.handle_history_entry_response(event);
            }
            ThreadBufferedEvent::FeedbackSubmission(event) => {
                self.handle_feedback_thread_event(event);
            }
        }
        if needs_refresh {
            self.refresh_status_line();
        }
    }

    pub(super) fn handle_thread_event_replay(&mut self, event: ThreadBufferedEvent) {
        match event {
            ThreadBufferedEvent::Notification(notification) => self
                .chat_widget
                .handle_server_notification(notification, Some(ReplayKind::ThreadSnapshot)),
            ThreadBufferedEvent::Request(request) => self
                .chat_widget
                .handle_server_request(request, Some(ReplayKind::ThreadSnapshot)),
            ThreadBufferedEvent::HistoryEntryResponse(event) => {
                self.chat_widget.handle_history_entry_response(event)
            }
            ThreadBufferedEvent::FeedbackSubmission(event) => {
                self.handle_feedback_thread_event(event);
            }
        }
    }

    /// Handles an event emitted by the currently active thread.
    ///
    /// This function enforces shutdown intent routing: unexpected non-primary
    /// thread shutdowns fail over to the primary thread, while user-requested
    /// app exits consume only the tracked shutdown completion and then proceed.
    pub(super) async fn handle_active_thread_event(
        &mut self,
        tui: &mut tui::Tui,
        app_server: &mut AppServerSession,
        event: ThreadBufferedEvent,
    ) -> Result<()> {
        // Capture this before any potential thread switch: we only want to clear
        // the exit marker when the currently active thread acknowledges shutdown.
        let pending_shutdown_exit_completed = matches!(
            &event,
            ThreadBufferedEvent::Notification(ServerNotification::ThreadClosed(_))
        ) && self.pending_shutdown_exit_thread_id
            == self.active_thread_id;

        // Processing order matters:
        //
        // 1. handle unexpected non-primary shutdown failover first;
        // 2. clear pending exit marker for matching shutdown;
        // 3. forward the event through normal handling.
        //
        // This preserves the mental model that user-requested exits do not trigger
        // failover, while true sub-agent deaths still do.
        if let ThreadBufferedEvent::Notification(notification) = &event
            && let Some((closed_thread_id, primary_thread_id)) =
                self.active_non_primary_shutdown_target(notification)
        {
            if let Some(channel) = self.thread_event_channels.get(&closed_thread_id) {
                let mut store = channel.store.lock().await;
                store.push_notification(notification.clone());
            }
            let should_wake_primary =
                self.should_wake_primary_for_closed_subagent(closed_thread_id);
            let closed_agent_label = self.subagent_activity_label(closed_thread_id);
            let closed_state_changed = self.mark_agent_picker_thread_closed(closed_thread_id);
            if self.side_threads.contains_key(&closed_thread_id) {
                self.discard_closed_side_thread(closed_thread_id).await;
                self.select_agent_thread_without_drain(tui, app_server, primary_thread_id)
                    .await?;
            } else {
                self.select_agent_thread_and_discard_side_without_drain(
                    tui,
                    app_server,
                    primary_thread_id,
                )
                .await?;
            }
            if self.active_thread_id == Some(primary_thread_id) {
                self.chat_widget.add_info_message(
                    format!(
                        "Agent thread {closed_thread_id} closed. Switched back to main thread."
                    ),
                    /*hint*/ None,
                );
                if should_wake_primary && closed_state_changed {
                    self.queue_or_submit_subagent_completion_wakeup(
                        closed_agent_label,
                        closed_thread_id,
                    )
                    .await;
                }
            } else {
                self.clear_active_thread().await;
                self.chat_widget.add_error_message(format!(
                    "Agent thread {closed_thread_id} closed. Failed to switch back to main thread {primary_thread_id}.",
                ));
            }
            return Ok(());
        }

        if pending_shutdown_exit_completed {
            // Clear only after seeing the shutdown completion for the tracked
            // thread, so unrelated shutdowns cannot consume this marker.
            self.pending_shutdown_exit_thread_id = None;
        }
        if self
            .should_drop_stopped_background_terminal_event(&event)
            .await
        {
            self.sync_background_terminal_activity_summaries().await;
            return Ok(());
        }
        if let ThreadBufferedEvent::Notification(notification) = &event {
            self.hydrate_collab_agent_metadata_for_notification(app_server, notification)
                .await;
        }

        self.handle_thread_event_now(event);
        self.sync_background_terminal_activity_summaries().await;
        if self.backtrack_render_pending {
            tui.frame_requester().schedule_frame();
        }
        Ok(())
    }
}
