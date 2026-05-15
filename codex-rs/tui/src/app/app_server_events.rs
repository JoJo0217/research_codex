//! App-server event stream handling for the TUI app.

use super::App;
use super::app_server_event_targets::ServerNotificationThreadTarget;
use super::app_server_event_targets::server_notification_thread_target;
use super::app_server_event_targets::server_request_thread_id;
use super::thread_routing::ThreadRequestEnqueueResult;
use crate::app_command::AppCommand;
use crate::app_event::AppEvent;
use crate::app_server_session::AppServerSession;
use crate::app_server_session::status_account_display_from_auth_mode;
use codex_app_server_client::AppServerEvent;
use codex_app_server_protocol::AuthMode;
use codex_app_server_protocol::ServerNotification;
use codex_app_server_protocol::ServerRequest;

impl App {
    fn refresh_mcp_startup_expected_servers_from_config(&mut self) {
        let enabled_config_mcp_servers: Vec<String> = self
            .chat_widget
            .config_ref()
            .mcp_servers
            .get()
            .iter()
            .filter_map(|(name, server)| server.enabled.then_some(name.clone()))
            .collect();
        self.chat_widget
            .set_mcp_startup_expected_servers(enabled_config_mcp_servers);
    }

    pub(super) async fn handle_app_server_event(
        &mut self,
        app_server_client: &AppServerSession,
        event: AppServerEvent,
    ) {
        match event {
            AppServerEvent::Lagged { skipped } => {
                tracing::warn!(
                    skipped,
                    "app-server event consumer lagged; dropping ignored events"
                );
                self.refresh_mcp_startup_expected_servers_from_config();
                self.chat_widget.finish_mcp_startup_after_lag();
            }
            AppServerEvent::ServerNotification(notification) => {
                self.handle_server_notification_event(app_server_client, notification)
                    .await;
            }
            AppServerEvent::ServerRequest(request) => {
                self.handle_server_request_event(app_server_client, request)
                    .await;
            }
            AppServerEvent::Disconnected { message } => {
                tracing::warn!("app-server event stream disconnected: {message}");
                self.chat_widget.add_error_message(message.clone());
                self.app_event_tx.send(AppEvent::FatalExitRequest(message));
            }
        }
    }

    async fn handle_server_notification_event(
        &mut self,
        app_server_client: &AppServerSession,
        notification: ServerNotification,
    ) {
        match &notification {
            ServerNotification::ServerRequestResolved(notification) => {
                if let Some(request) = self
                    .pending_app_server_requests
                    .resolve_notification(&notification.request_id)
                {
                    self.chat_widget.dismiss_app_server_request(&request);
                }
            }
            ServerNotification::McpServerStatusUpdated(_) => {
                self.refresh_mcp_startup_expected_servers_from_config();
            }
            ServerNotification::AccountRateLimitsUpdated(notification) => {
                self.chat_widget
                    .on_rate_limit_snapshot(Some(notification.rate_limits.clone()));
                return;
            }
            ServerNotification::AccountUpdated(notification) => {
                self.chat_widget.update_account_state(
                    status_account_display_from_auth_mode(
                        notification.auth_mode,
                        notification.plan_type,
                    ),
                    notification.plan_type,
                    matches!(
                        notification.auth_mode,
                        Some(AuthMode::Chatgpt) | Some(AuthMode::ChatgptAuthTokens)
                    ),
                );
                return;
            }
            ServerNotification::ExternalAgentConfigImportCompleted(_) => {
                let cwd = self.chat_widget.discovery_cwd().to_path_buf();
                if let Err(err) = self
                    .refresh_in_memory_config_from_disk_for_discovery_cwd(cwd.clone())
                    .await
                {
                    tracing::warn!(
                        error = %err,
                        "failed to refresh config after external agent config import"
                    );
                }
                self.chat_widget.refresh_plugin_mentions();
                self.chat_widget.submit_op(AppCommand::reload_user_config());
                self.fetch_plugins_list(app_server_client, cwd);
                return;
            }
            _ => {}
        }

        match server_notification_thread_target(&notification) {
            ServerNotificationThreadTarget::Thread(thread_id) => {
                let result = if self.primary_thread_id == Some(thread_id)
                    || self.primary_thread_id.is_none()
                {
                    self.enqueue_primary_thread_notification(notification).await
                } else {
                    self.enqueue_thread_notification(thread_id, notification)
                        .await
                };

                if let Err(err) = result {
                    tracing::warn!("failed to enqueue app-server notification: {err}");
                }
                return;
            }
            ServerNotificationThreadTarget::InvalidThreadId(thread_id) => {
                tracing::warn!(
                    thread_id,
                    "ignoring app-server notification with invalid thread_id"
                );
                return;
            }
            ServerNotificationThreadTarget::Global => {}
        }

        self.chat_widget
            .handle_server_notification(notification, /*replay_kind*/ None);
    }

    async fn handle_server_request_event(
        &mut self,
        app_server_client: &AppServerSession,
        request: ServerRequest,
    ) {
        if let Some(unsupported) =
            super::app_server_requests::PendingAppServerRequests::unsupported_server_request(
                &request,
            )
        {
            tracing::warn!(
                request_id = ?unsupported.request_id,
                message = unsupported.message,
                "rejecting unsupported app-server request"
            );
            self.chat_widget
                .add_error_message(unsupported.message.clone());
            if let Err(err) = self
                .reject_app_server_request(
                    app_server_client,
                    unsupported.request_id,
                    unsupported.message,
                )
                .await
            {
                tracing::warn!("{err}");
            }
            return;
        }

        let Some(thread_id) = server_request_thread_id(&request) else {
            let message =
                "Threadless app-server requests are not available in TUI yet.".to_string();
            tracing::warn!(
                request_id = ?request.id(),
                "rejecting threadless app-server request"
            );
            self.chat_widget.add_error_message(message.clone());
            if let Err(err) = self
                .reject_app_server_request(app_server_client, request.id().clone(), message)
                .await
            {
                tracing::warn!("{err}");
            }
            return;
        };

        if self.thread_is_closed_or_marked_closed(thread_id).await {
            let message = format!("Thread {thread_id} is closed.");
            tracing::warn!(
                thread_id = %thread_id,
                request_id = ?request.id(),
                "rejecting app-server request for closed thread"
            );
            if let Err(err) = self
                .reject_app_server_request(app_server_client, request.id().clone(), message)
                .await
            {
                tracing::warn!("{err}");
            }
            return;
        }

        if let Some(unsupported) = self
            .pending_app_server_requests
            .note_server_request(thread_id, &request)
        {
            tracing::warn!(
                request_id = ?unsupported.request_id,
                message = unsupported.message,
                "rejecting unsupported app-server request"
            );
            self.chat_widget
                .add_error_message(unsupported.message.clone());
            if let Err(err) = self
                .reject_app_server_request(
                    app_server_client,
                    unsupported.request_id,
                    unsupported.message,
                )
                .await
            {
                tracing::warn!("{err}");
            }
            return;
        }

        let result =
            if self.primary_thread_id == Some(thread_id) || self.primary_thread_id.is_none() {
                self.enqueue_primary_thread_request(request.clone()).await
            } else {
                self.enqueue_thread_request(thread_id, request.clone())
                    .await
            };
        match result {
            Ok(ThreadRequestEnqueueResult::Enqueued) => {}
            Ok(ThreadRequestEnqueueResult::ThreadClosed) => {
                let message = format!("Thread {thread_id} is closed.");
                self.pending_app_server_requests
                    .discard_server_request(request.id());
                tracing::warn!(
                    thread_id = %thread_id,
                    request_id = ?request.id(),
                    "rejecting app-server request for thread closed during enqueue"
                );
                if let Err(err) = self
                    .reject_app_server_request(app_server_client, request.id().clone(), message)
                    .await
                {
                    tracing::warn!("{err}");
                }
            }
            Err(err) => {
                tracing::warn!("failed to enqueue app-server request: {err}");
            }
        }
    }
}
