//! SQLx-backed session repository.

use std::{collections::BTreeMap, env, str::FromStr, time::Duration};

use centaur_session_core::{
    ExecutionStatus, HarnessType, MessageRole, SandboxCapabilities, SandboxRepoCacheAccess,
    Session, SessionEvent, SessionExecution, SessionMessage, SessionMessageInput, SessionStatus,
    ThreadKey, empty_object,
};
use serde::Deserialize;
use serde_json::Value;
use sqlx::{
    Acquire, FromRow, PgPool,
    postgres::{PgListener, PgPoolOptions, Postgres},
    types::Json,
};
use thiserror::Error;
use time::{Duration as TimeDuration, OffsetDateTime};
use uuid::Uuid;

// The API binary embeds these migrations at compile time.
static MIGRATOR: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

pub const SESSION_EVENTS_CHANNEL: &str = "centaur_session_events";
const SESSION_OUTPUT_LINE_EVENT: &str = "session.output.line";
const EXECUTION_METRICS_SCOPE_ENV: &str = "CENTAUR_EXECUTION_METRICS_ORGANIZATION_SCOPE";
const EXECUTION_METRIC_MAX_DURATION_MS: i64 = 86_400_000;
const DEFAULT_MAX_CONNECTIONS: u32 = 500;

#[derive(Clone, Debug, Eq, PartialEq)]
struct ParsedCommandMetricEvent {
    item_id: String,
    timestamp_ms: i64,
    command: Option<String>,
    status: Option<String>,
    completed: bool,
}

#[derive(Clone, Debug)]
pub struct CreateExecutionResult {
    pub execution: SessionExecution,
    pub created: bool,
}

#[derive(Clone, Debug)]
pub struct ClaimExecutionResult {
    pub execution: SessionExecution,
    /// True only when this call transitioned the execution from `queued` to
    /// `running`. False means another request already claimed it (or it is
    /// terminal), so the caller must not drive the execution.
    pub claimed: bool,
}

/// An active execution whose stdout-owner lease was released by
/// [`PgSessionStore::release_stdout_owned_executions`].
#[derive(Clone, Debug)]
pub struct ReleasedExecution {
    pub execution_id: String,
    pub thread_key: ThreadKey,
}

/// An active execution together with its stdout-owner lease state, as
/// returned by [`PgSessionStore::list_active_executions_with_ownership`].
/// The lease snapshot is advisory — only the conditional
/// `claim_expired_stdout_owner` update decides ownership — but it lets an
/// adoption scan skip executions with a live owner without touching the
/// session row or the sandbox backend.
#[derive(Clone, Debug)]
pub struct ActiveExecutionOwnership {
    pub execution: SessionExecution,
    pub stdout_owner_id: Option<String>,
    /// True when a stdout-owner lease exists and has not expired yet.
    pub stdout_owner_lease_active: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct IdleSandboxCandidate {
    pub thread_key: ThreadKey,
    pub sandbox_id: String,
    pub execution_id: String,
    pub idle_timeout: Duration,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SandboxCapacityCandidate {
    pub thread_key: ThreadKey,
    pub sandbox_id: String,
    pub latest_execution_id: Option<String>,
    pub last_active_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkflowOwnedSandbox {
    pub thread_key: ThreadKey,
    pub sandbox_id: String,
}

#[derive(Clone)]
pub struct PgSessionStore {
    pool: PgPool,
}

impl PgSessionStore {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn connect(database_url: &str) -> Result<Self, SessionStoreError> {
        let pool = PgPoolOptions::new()
            .max_connections(DEFAULT_MAX_CONNECTIONS)
            .connect(database_url)
            .await?;
        Ok(Self::new(pool))
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn run_migrations(&self) -> Result<(), SessionStoreError> {
        MIGRATOR.run(&self.pool).await?;
        Ok(())
    }

    pub async fn listen_session_events(&self) -> Result<SessionEventListener, SessionStoreError> {
        let mut listener = PgListener::connect_with(&self.pool).await?;
        listener.listen(SESSION_EVENTS_CHANNEL).await?;
        Ok(SessionEventListener { listener })
    }

    pub async fn create_or_get_session(
        &self,
        thread_key: &ThreadKey,
        harness_type: &HarnessType,
        persona_id: Option<&str>,
        metadata: Value,
        proxy_labels: BTreeMap<String, String>,
    ) -> Result<Session, SessionStoreError> {
        self.create_or_get_session_inner(
            thread_key,
            harness_type,
            persona_id,
            metadata,
            proxy_labels,
            false,
        )
        .await
    }

    /// Create or load a session while merging newly learned metadata into an
    /// existing row. The conflict update is skipped when every supplied value
    /// is already present, so unchanged calls do not write or touch updated_at.
    pub async fn create_or_get_session_merging_metadata(
        &self,
        thread_key: &ThreadKey,
        harness_type: &HarnessType,
        persona_id: Option<&str>,
        metadata: Value,
        proxy_labels: BTreeMap<String, String>,
    ) -> Result<Session, SessionStoreError> {
        self.create_or_get_session_inner(
            thread_key,
            harness_type,
            persona_id,
            metadata,
            proxy_labels,
            true,
        )
        .await
    }

    async fn create_or_get_session_inner(
        &self,
        thread_key: &ThreadKey,
        harness_type: &HarnessType,
        persona_id: Option<&str>,
        metadata: Value,
        proxy_labels: BTreeMap<String, String>,
        merge_metadata: bool,
    ) -> Result<Session, SessionStoreError> {
        let query = if merge_metadata {
            sqlx::query(
                r#"
                insert into sessions (thread_key, harness_type, persona_id, status, metadata, proxy_labels)
                values ($1, $2, $3, $4, $5, $6)
                on conflict (thread_key) do update
                set metadata = sessions.metadata || excluded.metadata,
                    updated_at = now()
                where sessions.harness_type = excluded.harness_type
                  and sessions.persona_id is not distinct from excluded.persona_id
                  and not sessions.metadata @> excluded.metadata
                "#,
            )
        } else {
            sqlx::query(
                r#"
                insert into sessions (thread_key, harness_type, persona_id, status, metadata, proxy_labels)
                values ($1, $2, $3, $4, $5, $6)
                on conflict (thread_key) do nothing
                "#,
            )
        };
        query
            .bind(thread_key.as_str())
            .bind(harness_type.as_ref())
            .bind(persona_id)
            .bind(SessionStatus::Idle.as_ref())
            .bind(metadata)
            .bind(Json(proxy_labels.clone()))
            .execute(&self.pool)
            .await?;

        if !proxy_labels.is_empty() {
            sqlx::query(
                r#"
                update sessions
                set proxy_labels = $2, updated_at = now()
                where thread_key = $1 and proxy_labels = '{}'::jsonb
                "#,
            )
            .bind(thread_key.as_str())
            .bind(Json(proxy_labels))
            .execute(&self.pool)
            .await?;
        }

        let session = self.get_session(thread_key).await?;
        if session.harness_type != *harness_type {
            return Err(SessionStoreError::HarnessConflict {
                thread_key: thread_key.as_str().to_owned(),
                existing: session.harness_type.to_string(),
                requested: harness_type.as_ref().to_owned(),
            });
        }
        if session.persona_id.as_deref() != persona_id {
            return Err(SessionStoreError::PersonaConflict {
                thread_key: thread_key.as_str().to_owned(),
                existing: session.persona_id,
                requested: persona_id.map(str::to_owned),
            });
        }
        Ok(session)
    }

    pub async fn get_session(&self, thread_key: &ThreadKey) -> Result<Session, SessionStoreError> {
        let row = sqlx::query_as::<_, SessionRow>(
            r#"
            select thread_key, title, sandbox_id, sandbox_repo_cache_enabled, sandbox_repo_cache_access, sandbox_observability_enabled, harness_type, harness_thread_id, persona_id, status, iron_control_principal, proxy_labels, sandbox_last_active_at, created_at, updated_at
            from sessions
            where thread_key = $1
            "#,
        )
        .bind(thread_key.as_str())
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| SessionStoreError::NotFound {
            thread_key: thread_key.as_str().to_owned(),
        })?;

        row.try_into()
    }

    pub async fn get_session_title(
        &self,
        thread_key: &ThreadKey,
    ) -> Result<Option<String>, SessionStoreError> {
        let title = sqlx::query_scalar::<_, Option<String>>(
            r#"
            select title
            from sessions
            where thread_key = $1
            "#,
        )
        .bind(thread_key.as_str())
        .fetch_optional(&self.pool)
        .await?
        .flatten();

        Ok(title)
    }

    pub async fn append_messages(
        &self,
        thread_key: &ThreadKey,
        messages: &[SessionMessageInput],
    ) -> Result<Vec<String>, SessionStoreError> {
        let mut tx = self.pool.begin().await?;
        let mut message_ids = Vec::with_capacity(messages.len());

        for message in messages {
            let message_id = prefixed_id("msg");
            let parts = Value::Array(message.parts.clone());
            let persisted_message_id = sqlx::query_scalar::<_, String>(
                r#"
                insert into session_messages
                    (message_id, thread_key, client_message_id, role, parts, metadata)
                values ($1, $2, $3, $4, $5, $6)
                on conflict (thread_key, client_message_id)
                    where client_message_id is not null
                do update set client_message_id = excluded.client_message_id
                returning message_id
                "#,
            )
            .bind(&message_id)
            .bind(thread_key.as_str())
            .bind(message.client_message_id.as_deref())
            .bind(message.role.as_ref())
            .bind(parts)
            .bind(message.metadata.clone())
            .fetch_one(&mut *tx)
            .await?;
            message_ids.push(persisted_message_id);
        }

        tx.commit().await?;
        Ok(message_ids)
    }

    pub async fn title_generation_candidate(
        &self,
        thread_key: &ThreadKey,
    ) -> Result<Option<Vec<Value>>, SessionStoreError> {
        let rows = sqlx::query_scalar::<_, Value>(
            r#"
            select m.parts
            from sessions s
            join session_messages m on m.thread_key = s.thread_key
            where s.thread_key = $1 and s.title is null
                and m.role = $2
            order by m.created_at, m.message_id
            "#,
        )
        .bind(thread_key.as_str())
        .bind(MessageRole::User.as_ref())
        .fetch_all(&self.pool)
        .await?;

        if rows.is_empty() {
            return Ok(None);
        }

        let parts = rows
            .into_iter()
            .flat_map(|parts| match parts {
                Value::Array(parts) => parts,
                other => vec![other],
            })
            .collect();
        Ok(Some(parts))
    }

    pub async fn set_session_title_if_empty(
        &self,
        thread_key: &ThreadKey,
        title: &str,
    ) -> Result<bool, SessionStoreError> {
        let result = sqlx::query(
            r#"
            update sessions
            set title = $2, updated_at = now()
            where thread_key = $1 and title is null
            "#,
        )
        .bind(thread_key.as_str())
        .bind(title)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    pub async fn list_messages(
        &self,
        thread_key: &ThreadKey,
    ) -> Result<Vec<SessionMessage>, SessionStoreError> {
        let rows = sqlx::query_as::<_, SessionMessageRow>(
            r#"
            select message_id, client_message_id, thread_key, role, parts, metadata, created_at
            from session_messages
            where thread_key = $1
            order by created_at, message_id
            "#,
        )
        .bind(thread_key.as_str())
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(TryInto::try_into).collect()
    }

    pub async fn create_execution(
        &self,
        thread_key: &ThreadKey,
        idempotency_key: Option<&str>,
        metadata: Value,
    ) -> Result<CreateExecutionResult, SessionStoreError> {
        self.create_execution_with_request(thread_key, idempotency_key, metadata, empty_object())
            .await
    }

    pub async fn create_execution_with_request(
        &self,
        thread_key: &ThreadKey,
        idempotency_key: Option<&str>,
        metadata: Value,
        request: Value,
    ) -> Result<CreateExecutionResult, SessionStoreError> {
        let execution_id = prefixed_id("exe");
        let row = sqlx::query_as::<_, CreateExecutionRow>(
            r#"
            insert into session_executions
                (execution_id, thread_key, idempotency_key, status, metadata, request)
            values ($1, $2, $3, $4, $5, $6)
            on conflict (thread_key, idempotency_key)
                where idempotency_key is not null
            do update set
                idempotency_key = excluded.idempotency_key,
                request = case
                    when session_executions.request = '{}'::jsonb then excluded.request
                    else session_executions.request
                end
            returning
                execution_id = $1 as created,
                execution_id,
                idempotency_key,
                thread_key,
                status,
                metadata,
                error,
                created_at,
                updated_at,
                started_at,
                completed_at
            "#,
        )
        .bind(&execution_id)
        .bind(thread_key.as_str())
        .bind(idempotency_key)
        .bind(ExecutionStatus::Queued.as_ref())
        .bind(metadata)
        .bind(request)
        .fetch_one(&self.pool)
        .await?;

        row.try_into()
    }

    pub async fn execution_request(&self, execution_id: &str) -> Result<Value, SessionStoreError> {
        sqlx::query_scalar::<_, Value>(
            r#"
            select request
            from session_executions
            where execution_id = $1
            "#,
        )
        .bind(execution_id)
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| SessionStoreError::ExecutionNotFound {
            execution_id: execution_id.to_owned(),
        })
    }

    /// Persist the execution trace context before input is delivered to the
    /// sandbox. Keeping it on the durable execution row lets recovery and
    /// steering continue the same trace after a control-plane restart.
    pub async fn set_execution_traceparent(
        &self,
        execution_id: &str,
        traceparent: &str,
    ) -> Result<(), SessionStoreError> {
        let updated = sqlx::query_scalar::<_, String>(
            r#"
            update session_executions
            set metadata = metadata || jsonb_build_object('centaur.traceparent', $2::text),
                updated_at = now()
            where execution_id = $1
            returning execution_id
            "#,
        )
        .bind(execution_id)
        .bind(traceparent)
        .fetch_optional(&self.pool)
        .await?;
        if updated.is_none() {
            return Err(SessionStoreError::ExecutionNotFound {
                execution_id: execution_id.to_owned(),
            });
        }
        Ok(())
    }

    pub async fn active_execution_for_thread(
        &self,
        thread_key: &ThreadKey,
    ) -> Result<Option<SessionExecution>, SessionStoreError> {
        let row = sqlx::query_as::<_, SessionExecutionRow>(
            r#"
            select execution_id, idempotency_key, thread_key, status, metadata, error, created_at, updated_at, started_at, completed_at
            from session_executions
            where thread_key = $1 and status in ($2, $3)
            order by created_at desc, execution_id desc
            limit 1
            "#,
        )
        .bind(thread_key.as_str())
        .bind(ExecutionStatus::Queued.as_ref())
        .bind(ExecutionStatus::Running.as_ref())
        .fetch_optional(&self.pool)
        .await?;

        row.map(TryInto::try_into).transpose()
    }

    /// Lists every execution still marked queued or running. Used at startup
    /// to adopt executions orphaned by a previous control plane process.
    pub async fn list_active_executions(&self) -> Result<Vec<SessionExecution>, SessionStoreError> {
        let rows = sqlx::query_as::<_, SessionExecutionRow>(
            r#"
            select execution_id, idempotency_key, thread_key, status, metadata, error, created_at, updated_at, started_at, completed_at
            from session_executions
            where status in ($1, $2)
            order by created_at, execution_id
            "#,
        )
        .bind(ExecutionStatus::Queued.as_ref())
        .bind(ExecutionStatus::Running.as_ref())
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(TryInto::try_into).collect()
    }

    pub async fn list_active_executions_with_ownership(
        &self,
    ) -> Result<Vec<ActiveExecutionOwnership>, SessionStoreError> {
        let rows = sqlx::query_as::<_, ActiveExecutionOwnershipRow>(
            r#"
            select execution_id, idempotency_key, thread_key, status, metadata, error, created_at, updated_at, started_at, completed_at,
                   stdout_owner_id,
                   coalesce(stdout_owner_lease_expires_at > now(), false) as stdout_owner_lease_active
            from session_executions
            where status in ($1, $2)
            order by created_at, execution_id
            "#,
        )
        .bind(ExecutionStatus::Queued.as_ref())
        .bind(ExecutionStatus::Running.as_ref())
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|row| {
                Ok(ActiveExecutionOwnership {
                    execution: row.execution.try_into()?,
                    stdout_owner_id: row.stdout_owner_id,
                    stdout_owner_lease_active: row.stdout_owner_lease_active,
                })
            })
            .collect()
    }

    pub async fn latest_execution_for_thread(
        &self,
        thread_key: &ThreadKey,
    ) -> Result<Option<SessionExecution>, SessionStoreError> {
        let row = sqlx::query_as::<_, SessionExecutionRow>(
            r#"
            select execution_id, idempotency_key, thread_key, status, metadata, error, created_at, updated_at, started_at, completed_at
            from session_executions
            where thread_key = $1
            order by created_at desc, execution_id desc
            limit 1
            "#,
        )
        .bind(thread_key.as_str())
        .fetch_optional(&self.pool)
        .await?;

        row.map(TryInto::try_into).transpose()
    }

    pub async fn mark_execution_running(
        &self,
        execution_id: &str,
    ) -> Result<ClaimExecutionResult, SessionStoreError> {
        let maybe_row = sqlx::query_as::<_, SessionExecutionRow>(
            r#"
            update session_executions
            set status = $2, started_at = coalesce(started_at, now()), updated_at = now()
            where execution_id = $1 and status = $3
            returning execution_id, idempotency_key, thread_key, status, metadata, error, created_at, updated_at, started_at, completed_at
            "#,
        )
        .bind(execution_id)
        .bind(ExecutionStatus::Running.as_ref())
        .bind(ExecutionStatus::Queued.as_ref())
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = maybe_row else {
            // The execution was not queued: a concurrent request already
            // claimed it or it reached a terminal state. Report the current
            // row without taking ownership.
            let row = sqlx::query_as::<_, SessionExecutionRow>(
                r#"
                select execution_id, idempotency_key, thread_key, status, metadata, error, created_at, updated_at, started_at, completed_at
                from session_executions
                where execution_id = $1
                "#,
            )
            .bind(execution_id)
            .fetch_one(&self.pool)
            .await?;
            return Ok(ClaimExecutionResult {
                execution: row.try_into()?,
                claimed: false,
            });
        };

        self.set_session_status(&row.thread_key, SessionStatus::Executing)
            .await?;
        Ok(ClaimExecutionResult {
            execution: row.try_into()?,
            claimed: true,
        })
    }

    pub async fn requeue_execution_if_running_without_stdout_owner(
        &self,
        execution_id: &str,
    ) -> Result<Option<SessionExecution>, SessionStoreError> {
        let row = sqlx::query_as::<_, SessionExecutionRow>(
            r#"
            update session_executions
            set status = $2, started_at = null, updated_at = now()
            where execution_id = $1
              and status = $3
              and stdout_owner_id is null
            returning execution_id, idempotency_key, thread_key, status, metadata, error, created_at, updated_at, started_at, completed_at
            "#,
        )
        .bind(execution_id)
        .bind(ExecutionStatus::Queued.as_ref())
        .bind(ExecutionStatus::Running.as_ref())
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };
        self.set_session_status(&row.thread_key, SessionStatus::Idle)
            .await?;
        row.try_into().map(Some)
    }

    pub async fn claim_stdout_owner(
        &self,
        execution_id: &str,
        owner_id: &str,
        lease: Duration,
    ) -> Result<bool, SessionStoreError> {
        let lease_expires_at = stdout_lease_expires_at(lease);
        let result = sqlx::query(
            r#"
            update session_executions
            set stdout_owner_id = $2,
                stdout_owner_lease_expires_at = $3,
                updated_at = now()
            where execution_id = $1
              and status in ($4, $5)
              and (
                stdout_owner_id is null
                or stdout_owner_id = $2
                or stdout_owner_lease_expires_at < now()
              )
            "#,
        )
        .bind(execution_id)
        .bind(owner_id)
        .bind(lease_expires_at)
        .bind(ExecutionStatus::Queued.as_ref())
        .bind(ExecutionStatus::Running.as_ref())
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    pub async fn claim_expired_stdout_owner(
        &self,
        execution_id: &str,
        owner_id: &str,
        lease: Duration,
    ) -> Result<bool, SessionStoreError> {
        let lease_expires_at = stdout_lease_expires_at(lease);
        let result = sqlx::query(
            r#"
            update session_executions
            set stdout_owner_id = $2,
                stdout_owner_lease_expires_at = $3,
                updated_at = now()
            where execution_id = $1
              and status in ($4, $5)
              and (
                stdout_owner_id is null
                or stdout_owner_lease_expires_at < now()
              )
            "#,
        )
        .bind(execution_id)
        .bind(owner_id)
        .bind(lease_expires_at)
        .bind(ExecutionStatus::Queued.as_ref())
        .bind(ExecutionStatus::Running.as_ref())
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    pub async fn renew_stdout_owner(
        &self,
        execution_id: &str,
        owner_id: &str,
        lease: Duration,
    ) -> Result<bool, SessionStoreError> {
        let lease_expires_at = stdout_lease_expires_at(lease);
        let result = sqlx::query(
            r#"
            update session_executions
            set stdout_owner_lease_expires_at = $3,
                updated_at = now()
            where execution_id = $1
              and stdout_owner_id = $2
              and status in ($4, $5)
            "#,
        )
        .bind(execution_id)
        .bind(owner_id)
        .bind(lease_expires_at)
        .bind(ExecutionStatus::Queued.as_ref())
        .bind(ExecutionStatus::Running.as_ref())
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    pub async fn release_stdout_owner(
        &self,
        execution_id: &str,
        owner_id: &str,
    ) -> Result<bool, SessionStoreError> {
        let result = sqlx::query(
            r#"
            update session_executions
            set stdout_owner_id = null,
                stdout_owner_lease_expires_at = null,
                updated_at = now()
            where execution_id = $1 and stdout_owner_id = $2
            "#,
        )
        .bind(execution_id)
        .bind(owner_id)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    pub async fn count_executions_with_stdout_owner(
        &self,
        owner_id: &str,
    ) -> Result<u64, SessionStoreError> {
        let count = sqlx::query_scalar::<_, i64>(
            r#"
            select count(*)
            from session_executions
            where stdout_owner_id = $1 and status in ($2, $3)
            "#,
        )
        .bind(owner_id)
        .bind(ExecutionStatus::Queued.as_ref())
        .bind(ExecutionStatus::Running.as_ref())
        .fetch_one(&self.pool)
        .await?;

        Ok(u64::try_from(count).unwrap_or_default())
    }

    /// Releases every active stdout-owner lease held by `owner_id` in one
    /// statement, returning the affected executions. Used by a clean
    /// control-plane shutdown so a peer's adoption scan can claim the
    /// executions immediately instead of waiting out the lease TTL.
    pub async fn release_stdout_owned_executions(
        &self,
        owner_id: &str,
    ) -> Result<Vec<ReleasedExecution>, SessionStoreError> {
        let rows = sqlx::query_as::<_, (String, String)>(
            r#"
            update session_executions
            set stdout_owner_id = null,
                stdout_owner_lease_expires_at = null,
                updated_at = now()
            where stdout_owner_id = $1 and status in ($2, $3)
            returning execution_id, thread_key
            "#,
        )
        .bind(owner_id)
        .bind(ExecutionStatus::Queued.as_ref())
        .bind(ExecutionStatus::Running.as_ref())
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter()
            .map(|(execution_id, thread_key)| {
                Ok(ReleasedExecution {
                    execution_id,
                    thread_key: parse_persisted(thread_key)?,
                })
            })
            .collect()
    }

    pub async fn complete_execution(
        &self,
        execution_id: &str,
    ) -> Result<SessionExecution, SessionStoreError> {
        let row = sqlx::query_as::<_, SessionExecutionRow>(
            r#"
            update session_executions
            set status = $2, completed_at = coalesce(completed_at, now()), updated_at = now()
            where execution_id = $1
            returning execution_id, idempotency_key, thread_key, status, metadata, error, created_at, updated_at, started_at, completed_at
            "#,
        )
        .bind(execution_id)
        .bind(ExecutionStatus::Completed.as_ref())
        .fetch_one(&self.pool)
        .await?;

        self.set_session_status(&row.thread_key, SessionStatus::Idle)
            .await?;
        row.try_into()
    }

    pub async fn complete_execution_if_active(
        &self,
        execution_id: &str,
    ) -> Result<Option<SessionExecution>, SessionStoreError> {
        let row = sqlx::query_as::<_, SessionExecutionRow>(
            r#"
            update session_executions
            set status = $2, completed_at = coalesce(completed_at, now()), updated_at = now()
            where execution_id = $1 and status in ($3, $4)
            returning execution_id, idempotency_key, thread_key, status, metadata, error, created_at, updated_at, started_at, completed_at
            "#,
        )
        .bind(execution_id)
        .bind(ExecutionStatus::Completed.as_ref())
        .bind(ExecutionStatus::Queued.as_ref())
        .bind(ExecutionStatus::Running.as_ref())
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };
        self.set_session_status(&row.thread_key, SessionStatus::Idle)
            .await?;
        row.try_into().map(Some)
    }

    pub async fn complete_execution_if_active_and_stdout_owner(
        &self,
        execution_id: &str,
        owner_id: &str,
    ) -> Result<Option<SessionExecution>, SessionStoreError> {
        let row = sqlx::query_as::<_, SessionExecutionRow>(
            r#"
            update session_executions
            set status = $2,
                completed_at = coalesce(completed_at, now()),
                stdout_owner_id = null,
                stdout_owner_lease_expires_at = null,
                updated_at = now()
            where execution_id = $1
              and status in ($3, $4)
              and stdout_owner_id = $5
            returning execution_id, idempotency_key, thread_key, status, metadata, error, created_at, updated_at, started_at, completed_at
            "#,
        )
        .bind(execution_id)
        .bind(ExecutionStatus::Completed.as_ref())
        .bind(ExecutionStatus::Queued.as_ref())
        .bind(ExecutionStatus::Running.as_ref())
        .bind(owner_id)
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };
        self.set_session_status(&row.thread_key, SessionStatus::Idle)
            .await?;
        row.try_into().map(Some)
    }

    pub async fn fail_execution(
        &self,
        execution_id: &str,
        error: &str,
    ) -> Result<SessionExecution, SessionStoreError> {
        let row = sqlx::query_as::<_, SessionExecutionRow>(
            r#"
            update session_executions
            set status = $2, error = $3, completed_at = coalesce(completed_at, now()), updated_at = now()
            where execution_id = $1
            returning execution_id, idempotency_key, thread_key, status, metadata, error, created_at, updated_at, started_at, completed_at
            "#,
        )
        .bind(execution_id)
        .bind(ExecutionStatus::Failed.as_ref())
        .bind(error)
        .fetch_one(&self.pool)
        .await?;

        self.set_session_status(&row.thread_key, SessionStatus::Failed)
            .await?;
        row.try_into()
    }

    pub async fn fail_execution_if_active(
        &self,
        execution_id: &str,
        error: &str,
    ) -> Result<Option<SessionExecution>, SessionStoreError> {
        let row = sqlx::query_as::<_, SessionExecutionRow>(
            r#"
            update session_executions
            set status = $2, error = $3, completed_at = coalesce(completed_at, now()), updated_at = now()
            where execution_id = $1 and status in ($4, $5)
            returning execution_id, idempotency_key, thread_key, status, metadata, error, created_at, updated_at, started_at, completed_at
            "#,
        )
        .bind(execution_id)
        .bind(ExecutionStatus::Failed.as_ref())
        .bind(error)
        .bind(ExecutionStatus::Queued.as_ref())
        .bind(ExecutionStatus::Running.as_ref())
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };
        self.set_session_status(&row.thread_key, SessionStatus::Failed)
            .await?;
        row.try_into().map(Some)
    }

    pub async fn fail_execution_if_active_and_stdout_owner(
        &self,
        execution_id: &str,
        owner_id: &str,
        error: &str,
    ) -> Result<Option<SessionExecution>, SessionStoreError> {
        let row = sqlx::query_as::<_, SessionExecutionRow>(
            r#"
            update session_executions
            set status = $2,
                error = $3,
                completed_at = coalesce(completed_at, now()),
                stdout_owner_id = null,
                stdout_owner_lease_expires_at = null,
                updated_at = now()
            where execution_id = $1
              and status in ($4, $5)
              and stdout_owner_id = $6
            returning execution_id, idempotency_key, thread_key, status, metadata, error, created_at, updated_at, started_at, completed_at
            "#,
        )
        .bind(execution_id)
        .bind(ExecutionStatus::Failed.as_ref())
        .bind(error)
        .bind(ExecutionStatus::Queued.as_ref())
        .bind(ExecutionStatus::Running.as_ref())
        .bind(owner_id)
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };
        self.set_session_status(&row.thread_key, SessionStatus::Failed)
            .await?;
        row.try_into().map(Some)
    }

    pub async fn cancel_execution_if_active_and_stdout_owner(
        &self,
        execution_id: &str,
        owner_id: &str,
        reason: &str,
    ) -> Result<Option<SessionExecution>, SessionStoreError> {
        let row = sqlx::query_as::<_, SessionExecutionRow>(
            r#"
            update session_executions
            set status = $2,
                error = $3,
                completed_at = coalesce(completed_at, now()),
                stdout_owner_id = null,
                stdout_owner_lease_expires_at = null,
                updated_at = now()
            where execution_id = $1
              and status in ($4, $5)
              and stdout_owner_id = $6
            returning execution_id, idempotency_key, thread_key, status, metadata, error, created_at, updated_at, started_at, completed_at
            "#,
        )
        .bind(execution_id)
        .bind(ExecutionStatus::Cancelled.as_ref())
        .bind(reason)
        .bind(ExecutionStatus::Queued.as_ref())
        .bind(ExecutionStatus::Running.as_ref())
        .bind(owner_id)
        .fetch_optional(&self.pool)
        .await?;

        let Some(row) = row else {
            return Ok(None);
        };
        self.set_session_status(&row.thread_key, SessionStatus::Idle)
            .await?;
        row.try_into().map(Some)
    }

    pub async fn append_event(
        &self,
        thread_key: &ThreadKey,
        execution_id: Option<&str>,
        event_type: &str,
        payload: Value,
    ) -> Result<SessionEvent, SessionStoreError> {
        let row = sqlx::query_as::<_, SessionEventRow>(
            r#"
            insert into session_events (thread_key, execution_id, event_type, payload)
            values ($1, $2, $3, $4)
            returning event_id, thread_key, execution_id, event_type, payload, created_at
            "#,
        )
        .bind(thread_key.as_str())
        .bind(execution_id)
        .bind(event_type)
        .bind(payload)
        .fetch_one(&self.pool)
        .await?;

        row.try_into()
    }

    pub async fn append_event_if_stdout_owner(
        &self,
        thread_key: &ThreadKey,
        execution_id: &str,
        owner_id: &str,
        lease: Duration,
        event_type: &str,
        payload: Value,
    ) -> Result<Option<SessionEvent>, SessionStoreError> {
        let lease_expires_at = stdout_lease_expires_at(lease);
        let mut tx = self.pool.begin().await?;
        let result = sqlx::query(
            r#"
            update session_executions
            set stdout_owner_lease_expires_at = $3,
                updated_at = now()
            where execution_id = $1
              and stdout_owner_id = $2
              and status in ($4, $5)
              and thread_key = $6
            "#,
        )
        .bind(execution_id)
        .bind(owner_id)
        .bind(lease_expires_at)
        .bind(ExecutionStatus::Queued.as_ref())
        .bind(ExecutionStatus::Running.as_ref())
        .bind(thread_key.as_str())
        .execute(&mut *tx)
        .await?;

        if result.rows_affected() == 0 {
            tx.commit().await?;
            return Ok(None);
        }

        let row = sqlx::query_as::<_, SessionEventRow>(
            r#"
            insert into session_events (thread_key, execution_id, event_type, payload)
            values ($1, $2, $3, $4)
            returning event_id, thread_key, execution_id, event_type, payload, created_at
            "#,
        )
        .bind(thread_key.as_str())
        .bind(execution_id)
        .bind(event_type)
        .bind(payload)
        .fetch_one(&mut *tx)
        .await?;

        if event_type == SESSION_OUTPUT_LINE_EVENT {
            // Metrics are best-effort telemetry.  A missing deployment scope,
            // persona, or an unrecognised protocol shape must not block the
            // durable session event.
            record_execution_metric_best_effort(
                &mut tx,
                thread_key,
                execution_id,
                row.event_id,
                &row.payload,
                row.created_at,
            )
            .await;
        }

        tx.commit().await?;
        row.try_into().map(Some)
    }

    pub async fn list_events_after(
        &self,
        thread_key: &ThreadKey,
        after_event_id: i64,
        execution_id: Option<&str>,
        limit: i64,
    ) -> Result<Vec<SessionEvent>, SessionStoreError> {
        let rows = sqlx::query_as::<_, SessionEventRow>(
            r#"
            select event_id, thread_key, execution_id, event_type, payload, created_at
            from session_events
            where thread_key = $1
              and event_id > $2
              and ($3::text is null or execution_id = $3)
            order by event_id
            limit $4
            "#,
        )
        .bind(thread_key.as_str())
        .bind(after_event_id)
        .bind(execution_id)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(TryInto::try_into).collect()
    }

    pub async fn execution_event_exists(
        &self,
        execution_id: &str,
        event_type: &str,
    ) -> Result<bool, SessionStoreError> {
        let exists = sqlx::query_scalar::<_, bool>(
            r#"
            select exists (
                select 1
                from session_events
                where execution_id = $1
                  and event_type = $2
                limit 1
            )
            "#,
        )
        .bind(execution_id)
        .bind(event_type)
        .fetch_one(&self.pool)
        .await?;

        Ok(exists)
    }

    pub async fn list_referenced_sandbox_ids(&self) -> Result<Vec<String>, SessionStoreError> {
        let rows = sqlx::query_scalar::<_, String>(
            r#"
            select sandbox_id
            from sessions
            where sandbox_id is not null

            union

            select sandbox_id
            from session_warm_sandboxes
            where status in ('ready', 'claimed', 'evicting')
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(rows)
    }

    pub async fn list_idle_sandbox_candidates(
        &self,
        idle_backstop: Duration,
    ) -> Result<Vec<IdleSandboxCandidate>, SessionStoreError> {
        let rows = sqlx::query_as::<_, IdleSandboxCandidateRow>(
            r#"
            with latest as (
                select distinct on (thread_key)
                    execution_id,
                    thread_key,
                    status,
                    completed_at,
                    metadata
                from session_executions
                order by thread_key, created_at desc, execution_id desc
            )
            select
                s.thread_key,
                s.sandbox_id as sandbox_id,
                latest.execution_id,
                latest.completed_at,
                latest.metadata
            from sessions s
            join latest on latest.thread_key = s.thread_key
            where s.sandbox_id is not null
              and latest.status in ('completed', 'failed', 'cancelled')
              and latest.completed_at is not null
              and not exists (
                  select 1
                  from session_executions active
                  where active.thread_key = s.thread_key
                    and active.status in ('queued', 'running')
              )
            order by latest.completed_at, s.thread_key
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let now = OffsetDateTime::now_utc();
        rows.into_iter()
            .filter_map(|row| idle_candidate_from_row(row, idle_backstop, now).transpose())
            .collect()
    }

    pub async fn list_sandbox_capacity_candidates(
        &self,
        excluded_thread_key: Option<&ThreadKey>,
        hot_idle_grace: std::time::Duration,
        limit: i64,
    ) -> Result<Vec<SandboxCapacityCandidate>, SessionStoreError> {
        let rows = sqlx::query_as::<_, SandboxCapacityCandidateRow>(
            r#"
            with latest as (
                select distinct on (thread_key)
                    execution_id,
                    thread_key,
                    completed_at
                from session_executions
                order by thread_key, created_at desc, execution_id desc
            )
            select
                s.thread_key,
                s.sandbox_id as sandbox_id,
                latest.execution_id as latest_execution_id,
                coalesce(
                    s.sandbox_last_active_at,
                    latest.completed_at,
                    s.updated_at,
                    s.created_at
                ) as last_active_at
            from sessions s
            left join latest on latest.thread_key = s.thread_key
            where s.sandbox_id is not null
              and ($1::text is null or s.thread_key != $1)
              and not exists (
                  select 1
                  from lateral (
                      select e.event_type
                      from session_events e
                      where e.thread_key = s.thread_key
                        and e.payload->>'sandbox_id' = s.sandbox_id
                        and e.event_type in (
                            'session.sandbox_paused',
                            'session.sandbox_ready',
                            'session.sandbox_resumed'
                        )
                      order by e.created_at desc, e.event_id desc
                      limit 1
                  ) latest_sandbox_event
                  where latest_sandbox_event.event_type = 'session.sandbox_paused'
              )
              and coalesce(
                    s.sandbox_last_active_at,
                    latest.completed_at,
                    s.updated_at,
                    s.created_at
                  ) <= now() - ($2::float8 * interval '1 second')
              and not exists (
                  select 1
                  from session_executions active
                  where active.thread_key = s.thread_key
                    and active.status in ('queued', 'running')
              )
            order by last_active_at, s.thread_key
            limit $3
            "#,
        )
        .bind(excluded_thread_key.map(ThreadKey::as_str))
        .bind(hot_idle_grace.as_secs_f64())
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(TryInto::try_into).collect()
    }

    pub async fn list_workflow_owned_sandboxes(
        &self,
        workflow_run_id: &str,
    ) -> Result<Vec<WorkflowOwnedSandbox>, SessionStoreError> {
        let rows = sqlx::query_as::<_, WorkflowOwnedSandboxRow>(
            r#"
            select thread_key, sandbox_id as sandbox_id
            from sessions
            where sandbox_id is not null
              and metadata->>'workflow_owned_thread' = 'true'
              and metadata->>'workflow_run_id' = $1
            order by thread_key
            "#,
        )
        .bind(workflow_run_id)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(TryInto::try_into).collect()
    }

    pub async fn update_sandbox_id(
        &self,
        thread_key: &ThreadKey,
        sandbox_id: Option<&str>,
    ) -> Result<Session, SessionStoreError> {
        let row = sqlx::query_as::<_, SessionRow>(
            r#"
            update sessions
            set
                sandbox_id = $2,
                sandbox_repo_cache_enabled = null,
                sandbox_repo_cache_access = null,
                sandbox_observability_enabled = null,
                sandbox_last_active_at = case
                    when $2::text is null then null
                    else now()
                end,
                updated_at = now()
            where thread_key = $1
            returning thread_key, title, sandbox_id, sandbox_repo_cache_enabled, sandbox_repo_cache_access, sandbox_observability_enabled, harness_type, harness_thread_id, persona_id, status, iron_control_principal, proxy_labels, sandbox_last_active_at, created_at, updated_at
            "#,
        )
        .bind(thread_key.as_str())
        .bind(sandbox_id)
        .fetch_one(&self.pool)
        .await?;

        row.try_into()
    }

    pub async fn update_sandbox_assignment(
        &self,
        thread_key: &ThreadKey,
        sandbox_id: &str,
        capabilities: &SandboxCapabilities,
    ) -> Result<Session, SessionStoreError> {
        let row = sqlx::query_as::<_, SessionRow>(
            r#"
            update sessions
            set
                sandbox_id = $2,
                sandbox_repo_cache_enabled = $3,
                sandbox_repo_cache_access = $4,
                sandbox_observability_enabled = $5,
                -- Keep the deprecated column populated during rolling upgrades
                -- so older api-rs pods can read assignments made by this version.
                sandbox_api_server_enabled = true,
                sandbox_last_active_at = now(),
                updated_at = now()
            where thread_key = $1
            returning thread_key, title, sandbox_id, sandbox_repo_cache_enabled, sandbox_repo_cache_access, sandbox_observability_enabled, harness_type, harness_thread_id, persona_id, status, iron_control_principal, proxy_labels, sandbox_last_active_at, created_at, updated_at
            "#,
        )
        .bind(thread_key.as_str())
        .bind(sandbox_id)
        .bind(capabilities.repo_cache_enabled())
        .bind(capabilities.repo_cache.as_str())
        .bind(capabilities.observability_enabled)
        .fetch_one(&self.pool)
        .await?;

        row.try_into()
    }

    pub async fn clear_sandbox_id_if_matches(
        &self,
        thread_key: &ThreadKey,
        sandbox_id: &str,
    ) -> Result<bool, SessionStoreError> {
        let result = sqlx::query(
            r#"
            update sessions
            set
                sandbox_id = null,
                sandbox_repo_cache_enabled = null,
                sandbox_repo_cache_access = null,
                sandbox_observability_enabled = null,
                sandbox_last_active_at = null,
                updated_at = now()
            where thread_key = $1 and sandbox_id = $2
            "#,
        )
        .bind(thread_key.as_str())
        .bind(sandbox_id)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    /// Move an existing session onto a different harness. Clears the sandbox
    /// and harness thread state (they belong to the old harness) and resets
    /// the session to idle; messages and events are preserved.
    pub async fn switch_session_harness(
        &self,
        thread_key: &ThreadKey,
        harness_type: &HarnessType,
    ) -> Result<Session, SessionStoreError> {
        let row = sqlx::query_as::<_, SessionRow>(
            r#"
            update sessions
            set harness_type = $2,
                harness_thread_id = null,
                sandbox_id = null,
                sandbox_repo_cache_enabled = null,
                sandbox_repo_cache_access = null,
                sandbox_observability_enabled = null,
                sandbox_last_active_at = null,
                status = $3,
                updated_at = now()
            where thread_key = $1
            returning thread_key, title, sandbox_id, sandbox_repo_cache_enabled, sandbox_repo_cache_access, sandbox_observability_enabled, harness_type, harness_thread_id, persona_id, status, iron_control_principal, proxy_labels, sandbox_last_active_at, created_at, updated_at
            "#,
        )
        .bind(thread_key.as_str())
        .bind(harness_type.as_ref())
        .bind(SessionStatus::Idle.as_ref())
        .fetch_optional(&self.pool)
        .await?
        .ok_or_else(|| SessionStoreError::NotFound {
            thread_key: thread_key.as_str().to_owned(),
        })?;

        row.try_into()
    }

    pub async fn set_iron_control_principal(
        &self,
        thread_key: &ThreadKey,
        iron_control_principal: Option<&str>,
    ) -> Result<Session, SessionStoreError> {
        let row = sqlx::query_as::<_, SessionRow>(
            r#"
            update sessions
            set iron_control_principal = $2, updated_at = now()
            where thread_key = $1
            returning thread_key, title, sandbox_id, sandbox_repo_cache_enabled, sandbox_repo_cache_access, sandbox_observability_enabled, harness_type, harness_thread_id, persona_id, status, iron_control_principal, proxy_labels, sandbox_last_active_at, created_at, updated_at
            "#,
        )
        .bind(thread_key.as_str())
        .bind(iron_control_principal)
        .fetch_one(&self.pool)
        .await?;

        row.try_into()
    }

    /// Bind a principal to a session without allowing an existing binding to
    /// change. The conditional update makes concurrent first bindings atomic:
    /// one caller wins, and a caller selecting a different principal receives
    /// a conflict instead of rebinding the session.
    pub async fn bind_iron_control_principal(
        &self,
        thread_key: &ThreadKey,
        iron_control_principal: &str,
    ) -> Result<Session, SessionStoreError> {
        let row = sqlx::query_as::<_, SessionRow>(
            r#"
            update sessions
            set iron_control_principal = $2, updated_at = now()
            where thread_key = $1
              and (iron_control_principal is null or iron_control_principal = $2)
            returning thread_key, title, sandbox_id, sandbox_repo_cache_enabled, sandbox_repo_cache_access, sandbox_observability_enabled, harness_type, harness_thread_id, persona_id, status, iron_control_principal, proxy_labels, sandbox_last_active_at, created_at, updated_at
            "#,
        )
        .bind(thread_key.as_str())
        .bind(iron_control_principal)
        .fetch_optional(&self.pool)
        .await?;

        if let Some(row) = row {
            return row.try_into();
        }

        let session = self.get_session(thread_key).await?;
        match session.iron_control_principal {
            Some(existing) => Err(SessionStoreError::PrincipalConflict {
                thread_key: thread_key.as_str().to_owned(),
                existing,
                requested: iron_control_principal.to_owned(),
            }),
            None => Err(SessionStoreError::InvalidPersistedValue(format!(
                "session {} remained unbound after principal binding",
                thread_key.as_str()
            ))),
        }
    }

    pub async fn insert_ready_warm_sandbox(
        &self,
        sandbox_id: &str,
        workload_key: &str,
    ) -> Result<(), SessionStoreError> {
        sqlx::query(
            r#"
            insert into session_warm_sandboxes (sandbox_id, workload_key, status)
            values ($1, $2, 'ready')
            "#,
        )
        .bind(sandbox_id)
        .bind(workload_key)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn count_ready_warm_sandboxes(
        &self,
        workload_key: &str,
    ) -> Result<i64, SessionStoreError> {
        let count = sqlx::query_scalar::<_, i64>(
            r#"
            select count(*)::bigint
            from session_warm_sandboxes
            where workload_key = $1 and status = 'ready'
            "#,
        )
        .bind(workload_key)
        .fetch_one(&self.pool)
        .await?;
        Ok(count)
    }

    pub async fn list_ready_warm_sandbox_ids(&self) -> Result<Vec<String>, SessionStoreError> {
        let sandbox_ids = sqlx::query_scalar::<_, String>(
            r#"
            select sandbox_id
            from session_warm_sandboxes
            where status = 'ready'
            order by created_at, sandbox_id
            "#,
        )
        .fetch_all(&self.pool)
        .await?;
        Ok(sandbox_ids)
    }

    pub async fn claim_ready_warm_sandbox(
        &self,
        workload_key: &str,
        thread_key: &str,
    ) -> Result<Option<String>, SessionStoreError> {
        let sandbox_id = sqlx::query_scalar::<_, String>(
            r#"
            with candidate as (
                select sandbox_id
                from session_warm_sandboxes
                where workload_key = $1 and status = 'ready'
                order by created_at, sandbox_id
                for update skip locked
                limit 1
            )
            update session_warm_sandboxes warm
            set
                status = 'claimed',
                claimed_thread_key = $2,
                claimed_at = now(),
                updated_at = now()
            from candidate
            where warm.sandbox_id = candidate.sandbox_id
            returning warm.sandbox_id
            "#,
        )
        .bind(workload_key)
        .bind(thread_key)
        .fetch_optional(&self.pool)
        .await?;
        Ok(sandbox_id)
    }

    pub async fn reserve_ready_warm_sandboxes_for_eviction(
        &self,
        limit: i64,
    ) -> Result<Vec<String>, SessionStoreError> {
        let rows = sqlx::query_scalar::<_, String>(
            r#"
            with candidates as (
                select sandbox_id
                from session_warm_sandboxes
                where status = 'ready'
                order by created_at, sandbox_id
                for update skip locked
                limit $1
            )
            update session_warm_sandboxes warm
            set
                status = 'evicting',
                updated_at = now()
            from candidates
            where warm.sandbox_id = candidates.sandbox_id
            returning warm.sandbox_id
            "#,
        )
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub async fn list_stale_evicting_warm_sandbox_ids(
        &self,
        min_age: Duration,
    ) -> Result<Vec<String>, SessionStoreError> {
        let rows = sqlx::query_scalar::<_, String>(
            r#"
            select sandbox_id
            from session_warm_sandboxes
            where status = 'evicting'
              and updated_at <= now() - ($1::float8 * interval '1 second')
            order by updated_at, sandbox_id
            "#,
        )
        .bind(min_age.as_secs_f64())
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub async fn mark_warm_sandbox_failed(
        &self,
        sandbox_id: &str,
        error: &str,
    ) -> Result<(), SessionStoreError> {
        sqlx::query(
            r#"
            update session_warm_sandboxes
            set status = 'failed', last_error = $2, updated_at = now()
            where sandbox_id = $1
            "#,
        )
        .bind(sandbox_id)
        .bind(error)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    pub async fn update_harness_thread_id(
        &self,
        thread_key: &ThreadKey,
        harness_thread_id: Option<&str>,
    ) -> Result<Session, SessionStoreError> {
        let row = sqlx::query_as::<_, SessionRow>(
            r#"
            update sessions
            set harness_thread_id = $2, updated_at = now()
            where thread_key = $1
            returning thread_key, title, sandbox_id, sandbox_repo_cache_enabled, sandbox_repo_cache_access, sandbox_observability_enabled, harness_type, harness_thread_id, persona_id, status, iron_control_principal, proxy_labels, sandbox_last_active_at, created_at, updated_at
            "#,
        )
        .bind(thread_key.as_str())
        .bind(harness_thread_id)
        .fetch_one(&self.pool)
        .await?;

        row.try_into()
    }

    pub async fn touch_session_sandbox_activity(
        &self,
        thread_key: &ThreadKey,
    ) -> Result<bool, SessionStoreError> {
        let result = sqlx::query(
            r#"
            update sessions
            set sandbox_last_active_at = now()
            where thread_key = $1 and sandbox_id is not null
            "#,
        )
        .bind(thread_key.as_str())
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    pub async fn touch_sandbox_activity(
        &self,
        thread_key: &ThreadKey,
        sandbox_id: &str,
    ) -> Result<bool, SessionStoreError> {
        let result = sqlx::query(
            r#"
            update sessions
            set sandbox_last_active_at = now()
            where thread_key = $1 and sandbox_id = $2
            "#,
        )
        .bind(thread_key.as_str())
        .bind(sandbox_id)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    async fn set_session_status(
        &self,
        thread_key: &str,
        status: SessionStatus,
    ) -> Result<(), SessionStoreError> {
        sqlx::query(
            r#"
            update sessions
            set status = $2, updated_at = now()
            where thread_key = $1
            "#,
        )
        .bind(thread_key)
        .bind(status.as_ref())
        .execute(&self.pool)
        .await?;
        Ok(())
    }
}

pub struct SessionEventListener {
    listener: PgListener,
}

impl SessionEventListener {
    pub async fn recv(&mut self) -> Result<SessionEventNotification, SessionStoreError> {
        loop {
            let notification = self.listener.recv().await?;
            if notification.channel() != SESSION_EVENTS_CHANNEL {
                continue;
            }

            let payload = notification.payload();
            return serde_json::from_str(payload).map_err(|error| {
                SessionStoreError::InvalidNotification {
                    channel: notification.channel().to_owned(),
                    payload: payload.to_owned(),
                    error,
                }
            });
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct SessionEventNotification {
    pub thread_key: String,
    pub event_id: i64,
}

#[derive(Debug, Error)]
pub enum SessionStoreError {
    #[error("session not found for thread_key {thread_key}")]
    NotFound { thread_key: String },
    #[error(
        "session {thread_key} already exists with harness_type {existing}, requested {requested}"
    )]
    HarnessConflict {
        thread_key: String,
        existing: String,
        requested: String,
    },
    #[error(
        "session {thread_key} already exists with persona_id {existing:?}, requested {requested:?}"
    )]
    PersonaConflict {
        thread_key: String,
        existing: Option<String>,
        requested: Option<String>,
    },
    #[error("session {thread_key} already exists with principal {existing}, requested {requested}")]
    PrincipalConflict {
        thread_key: String,
        existing: String,
        requested: String,
    },
    #[error("invalid persisted value: {0}")]
    InvalidPersistedValue(String),
    #[error("session execution not found for execution_id {execution_id}")]
    ExecutionNotFound { execution_id: String },
    #[error("invalid notification payload on {channel}: {payload}: {error}")]
    InvalidNotification {
        channel: String,
        payload: String,
        error: serde_json::Error,
    },
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    #[error(transparent)]
    Migrate(#[from] sqlx::migrate::MigrateError),
}

#[derive(Debug, FromRow)]
struct SessionRow {
    thread_key: String,
    title: Option<String>,
    sandbox_id: Option<String>,
    sandbox_repo_cache_enabled: Option<bool>,
    sandbox_repo_cache_access: Option<String>,
    sandbox_observability_enabled: Option<bool>,
    harness_type: String,
    harness_thread_id: Option<String>,
    persona_id: Option<String>,
    status: String,
    iron_control_principal: Option<String>,
    proxy_labels: Json<BTreeMap<String, String>>,
    sandbox_last_active_at: Option<OffsetDateTime>,
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
}

impl TryFrom<SessionRow> for Session {
    type Error = SessionStoreError;

    fn try_from(row: SessionRow) -> Result<Self, Self::Error> {
        Ok(Self {
            thread_key: parse_persisted(row.thread_key)?,
            title: row.title,
            sandbox_id: row.sandbox_id,
            sandbox_capabilities: match (
                row.sandbox_repo_cache_enabled,
                row.sandbox_repo_cache_access,
                row.sandbox_observability_enabled,
            ) {
                (Some(repo_cache_enabled), repo_cache_access, Some(observability_enabled)) => {
                    Some(SandboxCapabilities {
                        repo_cache: repo_cache_access
                            .as_deref()
                            .and_then(SandboxRepoCacheAccess::parse)
                            .unwrap_or_else(|| {
                                SandboxRepoCacheAccess::from_legacy_enabled(repo_cache_enabled)
                            }),
                        observability_enabled,
                    })
                }
                _ => None,
            },
            harness_type: parse_persisted(row.harness_type)?,
            harness_thread_id: row.harness_thread_id,
            persona_id: row.persona_id,
            status: parse_persisted(row.status)?,
            iron_control_principal: row.iron_control_principal,
            proxy_labels: row.proxy_labels.0,
            sandbox_last_active_at: row.sandbox_last_active_at,
            created_at: row.created_at,
            updated_at: row.updated_at,
        })
    }
}

#[derive(Debug, FromRow)]
struct SessionMessageRow {
    message_id: String,
    client_message_id: Option<String>,
    thread_key: String,
    role: String,
    parts: Value,
    metadata: Value,
    created_at: OffsetDateTime,
}

impl TryFrom<SessionMessageRow> for SessionMessage {
    type Error = SessionStoreError;

    fn try_from(row: SessionMessageRow) -> Result<Self, Self::Error> {
        let parts = match row.parts {
            Value::Array(parts) => parts,
            other => vec![other],
        };
        Ok(Self {
            message_id: row.message_id,
            client_message_id: row.client_message_id,
            thread_key: parse_persisted(row.thread_key)?,
            role: parse_persisted(row.role)?,
            parts,
            metadata: row.metadata,
            created_at: row.created_at,
        })
    }
}

#[derive(Debug, FromRow)]
struct SessionExecutionRow {
    execution_id: String,
    idempotency_key: Option<String>,
    thread_key: String,
    status: String,
    metadata: Value,
    error: Option<String>,
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
    started_at: Option<OffsetDateTime>,
    completed_at: Option<OffsetDateTime>,
}

#[derive(Debug, FromRow)]
struct ActiveExecutionOwnershipRow {
    #[sqlx(flatten)]
    execution: SessionExecutionRow,
    stdout_owner_id: Option<String>,
    stdout_owner_lease_active: bool,
}

#[derive(Debug, FromRow)]
struct IdleSandboxCandidateRow {
    thread_key: String,
    sandbox_id: String,
    execution_id: String,
    completed_at: OffsetDateTime,
    metadata: Value,
}

fn idle_candidate_from_row(
    row: IdleSandboxCandidateRow,
    idle_backstop: Duration,
    now: OffsetDateTime,
) -> Result<Option<IdleSandboxCandidate>, SessionStoreError> {
    let idle_timeout = effective_idle_timeout(&row.metadata, idle_backstop);
    if !idle_deadline_elapsed(row.completed_at, idle_timeout, now) {
        return Ok(None);
    }
    Ok(Some(IdleSandboxCandidate {
        thread_key: parse_persisted(row.thread_key)?,
        sandbox_id: row.sandbox_id,
        execution_id: row.execution_id,
        idle_timeout,
    }))
}

fn effective_idle_timeout(metadata: &Value, idle_backstop: Duration) -> Duration {
    metadata
        .get("idle_timeout_ms")
        .and_then(Value::as_u64)
        .filter(|value| *value > 0)
        .map(Duration::from_millis)
        .unwrap_or_else(|| std::cmp::max(idle_backstop, Duration::from_millis(1)))
}

fn idle_deadline_elapsed(
    completed_at: OffsetDateTime,
    idle_timeout: Duration,
    now: OffsetDateTime,
) -> bool {
    let elapsed = now - completed_at;
    if elapsed.is_negative() {
        return false;
    }
    elapsed.whole_nanoseconds() >= idle_timeout.as_nanos() as i128
}

#[derive(Debug, FromRow)]
struct SandboxCapacityCandidateRow {
    thread_key: String,
    sandbox_id: String,
    latest_execution_id: Option<String>,
    last_active_at: OffsetDateTime,
}

impl TryFrom<SandboxCapacityCandidateRow> for SandboxCapacityCandidate {
    type Error = SessionStoreError;

    fn try_from(row: SandboxCapacityCandidateRow) -> Result<Self, Self::Error> {
        Ok(Self {
            thread_key: parse_persisted(row.thread_key)?,
            sandbox_id: row.sandbox_id,
            latest_execution_id: row.latest_execution_id,
            last_active_at: row.last_active_at,
        })
    }
}

#[derive(Debug, FromRow)]
struct WorkflowOwnedSandboxRow {
    thread_key: String,
    sandbox_id: String,
}

impl TryFrom<WorkflowOwnedSandboxRow> for WorkflowOwnedSandbox {
    type Error = SessionStoreError;

    fn try_from(row: WorkflowOwnedSandboxRow) -> Result<Self, Self::Error> {
        Ok(Self {
            thread_key: parse_persisted(row.thread_key)?,
            sandbox_id: row.sandbox_id,
        })
    }
}

impl TryFrom<SessionExecutionRow> for SessionExecution {
    type Error = SessionStoreError;

    fn try_from(row: SessionExecutionRow) -> Result<Self, Self::Error> {
        Ok(Self {
            execution_id: row.execution_id,
            idempotency_key: row.idempotency_key,
            thread_key: parse_persisted(row.thread_key)?,
            status: parse_persisted(row.status)?,
            metadata: row.metadata,
            error: row.error,
            created_at: row.created_at,
            updated_at: row.updated_at,
            started_at: row.started_at,
            completed_at: row.completed_at,
        })
    }
}

#[derive(Debug, FromRow)]
struct CreateExecutionRow {
    created: bool,
    execution_id: String,
    idempotency_key: Option<String>,
    thread_key: String,
    status: String,
    metadata: Value,
    error: Option<String>,
    created_at: OffsetDateTime,
    updated_at: OffsetDateTime,
    started_at: Option<OffsetDateTime>,
    completed_at: Option<OffsetDateTime>,
}

impl TryFrom<CreateExecutionRow> for CreateExecutionResult {
    type Error = SessionStoreError;

    fn try_from(row: CreateExecutionRow) -> Result<Self, Self::Error> {
        Ok(Self {
            created: row.created,
            execution: SessionExecutionRow {
                execution_id: row.execution_id,
                idempotency_key: row.idempotency_key,
                thread_key: row.thread_key,
                status: row.status,
                metadata: row.metadata,
                error: row.error,
                created_at: row.created_at,
                updated_at: row.updated_at,
                started_at: row.started_at,
                completed_at: row.completed_at,
            }
            .try_into()?,
        })
    }
}

#[derive(Debug, FromRow)]
struct SessionEventRow {
    event_id: i64,
    thread_key: String,
    execution_id: Option<String>,
    event_type: String,
    payload: Value,
    created_at: OffsetDateTime,
}

#[derive(Debug, FromRow)]
struct MetricEventPayloadRow {
    payload: Value,
}

impl TryFrom<SessionEventRow> for SessionEvent {
    type Error = SessionStoreError;

    fn try_from(row: SessionEventRow) -> Result<Self, Self::Error> {
        Ok(Self {
            event_id: row.event_id,
            thread_key: parse_persisted(row.thread_key)?,
            execution_id: row.execution_id,
            event_type: row.event_type,
            payload: row.payload,
            created_at: row.created_at,
        })
    }
}

fn parse_persisted<T>(value: String) -> Result<T, SessionStoreError>
where
    T: FromStr,
    T::Err: std::fmt::Display,
{
    value
        .parse()
        .map_err(|err: T::Err| SessionStoreError::InvalidPersistedValue(err.to_string()))
}

fn prefixed_id(prefix: &str) -> String {
    format!("{prefix}_{}", Uuid::new_v4().simple())
}

pub fn default_metadata(metadata: Option<Value>) -> Value {
    metadata.unwrap_or_else(empty_object)
}

fn stdout_lease_expires_at(lease: Duration) -> OffsetDateTime {
    let seconds = i64::try_from(lease.as_secs()).unwrap_or(i64::MAX);
    OffsetDateTime::now_utc() + TimeDuration::new(seconds, lease.subsec_nanos() as i32)
}

async fn record_execution_metric_best_effort(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    thread_key: &ThreadKey,
    execution_id: &str,
    completed_event_id: i64,
    payload: &Value,
    captured_at: OffsetDateTime,
) {
    // Keep metric ingestion behind a savepoint.  An unavailable metric table,
    // malformed deployment grant, or any other telemetry error must roll back
    // only the optional aggregate write and never abort the session event
    // transaction.
    let mut metrics_tx = match tx.begin().await {
        Ok(transaction) => transaction,
        Err(error) => {
            tracing::warn!(%error, "failed to start execution metric savepoint");
            return;
        }
    };
    match record_execution_metric(
        &mut metrics_tx,
        thread_key,
        execution_id,
        completed_event_id,
        payload,
        captured_at,
    )
    .await
    {
        Ok(()) => {
            if let Err(error) = metrics_tx.commit().await {
                tracing::warn!(%error, "failed to commit execution metric savepoint");
            }
        }
        Err(error) => {
            let _ = metrics_tx.rollback().await;
            tracing::warn!(%error, "failed to record execution metric");
        }
    }
}

async fn record_execution_metric<'a>(
    tx: &mut sqlx::Transaction<'a, Postgres>,
    thread_key: &ThreadKey,
    execution_id: &str,
    completed_event_id: i64,
    payload: &Value,
    captured_at: OffsetDateTime,
) -> Result<(), sqlx::Error> {
    let Some(completed) = parse_command_metric_event(payload, true) else {
        return Ok(());
    };
    let Some(status) = (match completed.status.as_deref() {
        Some("completed") => Some("completed"),
        Some("failed") => Some("failed"),
        _ => None,
    }) else {
        return Ok(());
    };

    let event_rows = sqlx::query_as::<_, MetricEventPayloadRow>(
        r#"
        select payload
          from session_events
         where execution_id = $1
           and event_type = $2
           and event_id < $3
         order by event_id desc
         limit 1000
        "#,
    )
    .bind(execution_id)
    .bind(SESSION_OUTPUT_LINE_EVENT)
    .bind(completed_event_id)
    .fetch_all(&mut **tx)
    .await?;

    let mut started = None;
    for row in event_rows {
        let Some(event) = parse_command_metric_event(&row.payload, false) else {
            continue;
        };
        if event.item_id != completed.item_id {
            continue;
        }
        if event.completed {
            // At-least-once stdout delivery must not inflate a bucket when the
            // same normalized completion is observed more than once.
            return Ok(());
        }
        started = Some(event);
        break;
    }
    let Some(started) = started else {
        return Ok(());
    };
    let duration_ms = completed
        .timestamp_ms
        .checked_sub(started.timestamp_ms)
        .filter(|duration| (1..=EXECUTION_METRIC_MAX_DURATION_MS).contains(duration));
    let Some(duration_ms) = duration_ms else {
        return Ok(());
    };
    let Some(command) = started.command.as_deref().or(completed.command.as_deref()) else {
        return Ok(());
    };
    let Some(command_family) = command_family(command) else {
        return Ok(());
    };
    let Some(organization_scope) = configured_execution_metric_scope() else {
        return Ok(());
    };
    let persona_key = sqlx::query_scalar::<_, Option<String>>(
        "select persona_id from sessions where thread_key = $1",
    )
    .bind(thread_key.as_str())
    .fetch_optional(&mut **tx)
    .await?
    .flatten()
    .and_then(|value| safe_dimension(&value));
    let Some(persona_key) = persona_key else {
        return Ok(());
    };

    sqlx::query(
        r#"
        insert into heartbeat_execution_metric_buckets (
            organization_scope, persona_key, language, command_family,
            bucket_start, duration_bucket_ms, status, sample_count,
            total_duration_ms
        ) values ($1, $2, 'rust', $3,
                  date_trunc('hour', $4::timestamptz at time zone 'UTC') at time zone 'UTC',
                  $5, $6, 1, $7)
        on conflict (organization_scope, persona_key, language, command_family,
                     bucket_start, duration_bucket_ms, status)
        do update set sample_count = heartbeat_execution_metric_buckets.sample_count + 1,
                      total_duration_ms = heartbeat_execution_metric_buckets.total_duration_ms
                                          + excluded.total_duration_ms
        "#,
    )
    .bind(organization_scope)
    .bind(persona_key)
    .bind(command_family)
    .bind(captured_at)
    .bind(duration_bucket(duration_ms))
    .bind(status)
    .bind(duration_ms)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

fn parse_command_metric_event(
    payload: &Value,
    completed: bool,
) -> Option<ParsedCommandMetricEvent> {
    let line = payload.as_str()?;
    let value: Value = serde_json::from_str(line).ok()?;
    let method = value.get("method").and_then(Value::as_str)?;
    let is_completed = matches!(method, "item/completed" | "item.completed");
    if is_completed != completed {
        return None;
    }
    let item = value
        .pointer("/params/item")
        .or_else(|| value.get("item"))?;
    if !matches!(
        item.get("type").and_then(Value::as_str),
        Some("commandExecution" | "command_execution")
    ) {
        return None;
    }
    let item_id = item.get("id").and_then(Value::as_str)?.to_owned();
    let timestamp_ms = [
        if completed {
            "/params/completedAtMs"
        } else {
            "/params/startedAtMs"
        },
        if completed {
            "/params/completed_at_ms"
        } else {
            "/params/started_at_ms"
        },
    ]
    .into_iter()
    .find_map(|path| value.pointer(path).and_then(Value::as_i64))?;
    Some(ParsedCommandMetricEvent {
        item_id,
        timestamp_ms,
        command: item
            .get("command")
            .and_then(Value::as_str)
            .map(str::to_owned),
        status: item
            .get("status")
            .and_then(Value::as_str)
            .map(str::to_owned),
        completed: is_completed,
    })
}

fn command_family(command: &str) -> Option<&'static str> {
    let mut words = shell_words::split(command).ok()?;
    if words.is_empty() {
        return None;
    }
    if matches!(executable_name(words.first()?), "bash" | "sh" | "zsh") {
        if !matches!(words.get(1).map(String::as_str), Some("-c" | "-lc")) || words.len() != 3 {
            return None;
        }
        words = shell_words::split(words.get(2)?).ok()?;
    }
    if words.is_empty()
        || words.iter().any(|word| {
            word.chars()
                .any(|ch| matches!(ch, ';' | '|' | '&' | '>' | '<' | '`'))
                || word.contains("$(")
        })
    {
        return None;
    }
    match executable_name(words.first()?) {
        "rustc"
            if words.len() > 1
                && !words.iter().skip(1).any(|word| {
                    matches!(word.as_str(), "--version" | "-V" | "--help" | "-h")
                        || word.starts_with("--version=")
                        || word.starts_with("--help=")
                        || word.starts_with("-V")
                }) =>
        {
            Some("rustc")
        }
        "cargo" => match words.get(1).map(String::as_str) {
            Some("build") => Some("cargo_build"),
            Some("check") => Some("cargo_check"),
            Some("test") => Some("cargo_test"),
            Some("clippy") => Some("cargo_clippy"),
            Some("doc") => Some("cargo_doc"),
            Some("bench") => Some("cargo_bench"),
            _ => None,
        },
        _ => None,
    }
}

fn executable_name(value: &str) -> &str {
    value.rsplit('/').next().unwrap_or(value)
}

fn configured_execution_metric_scope() -> Option<String> {
    env::var(EXECUTION_METRICS_SCOPE_ENV)
        .ok()
        .and_then(|value| safe_dimension(value.trim()))
}

fn safe_dimension(value: &str) -> Option<String> {
    if value.is_empty()
        || value.len() > 128
        || !value.chars().enumerate().all(|(index, ch)| {
            (index == 0 && ch.is_ascii_alphanumeric())
                || (index > 0
                    && (ch.is_ascii_alphanumeric() || matches!(ch, '_' | '.' | ':' | '-')))
        })
    {
        return None;
    }
    Some(value.to_owned())
}

fn duration_bucket(duration_ms: i64) -> i32 {
    [
        100, 250, 500, 1000, 2500, 5000, 10000, 30000, 60000, 300000, 900000, 3600000, 86400000,
    ]
    .into_iter()
    .find(|bucket| duration_ms <= i64::from(*bucket))
    .unwrap_or(86400000)
}

#[cfg(test)]
mod tests {
    use std::{collections::BTreeMap, time::Duration};

    use centaur_session_core::{HarnessType, ThreadKey};
    use serde_json::{Value, json};
    use time::{Duration as TimeDuration, OffsetDateTime};
    use uuid::Uuid;

    use super::{
        IdleSandboxCandidateRow, PgSessionStore, SESSION_OUTPUT_LINE_EVENT,
        SessionEventNotification, command_family, parse_command_metric_event, safe_dimension,
    };

    async fn test_store() -> Option<PgSessionStore> {
        let Ok(url) = std::env::var("SESSION_RUNTIME_TEST_DATABASE_URL") else {
            eprintln!("skipping: SESSION_RUNTIME_TEST_DATABASE_URL not set");
            return None;
        };
        let store = PgSessionStore::connect(&url)
            .await
            .expect("connect test db");
        store.run_migrations().await.expect("run migrations");
        Some(store)
    }

    #[test]
    fn parses_session_event_notification_payload() {
        let notification: SessionEventNotification =
            serde_json::from_str(r#"{"thread_key":"cli:test","event_id":42}"#).unwrap();

        assert_eq!(
            notification,
            SessionEventNotification {
                thread_key: "cli:test".to_owned(),
                event_id: 42,
            }
        );
    }

    fn idle_row(
        metadata: serde_json::Value,
        completed_at: OffsetDateTime,
    ) -> IdleSandboxCandidateRow {
        IdleSandboxCandidateRow {
            thread_key: "test:idle-row".to_owned(),
            sandbox_id: "sbx-idle-row".to_owned(),
            execution_id: "exe-idle-row".to_owned(),
            completed_at,
            metadata,
        }
    }

    #[test]
    fn idle_candidate_uses_persisted_timeout_deadline() {
        let now = OffsetDateTime::now_utc();
        let candidate = super::idle_candidate_from_row(
            idle_row(
                json!({"idle_timeout_ms": 1000}),
                now - TimeDuration::seconds(2),
            ),
            Duration::from_secs(3600),
            now,
        )
        .unwrap()
        .expect("candidate should use persisted timeout");

        assert_eq!(candidate.idle_timeout, Duration::from_secs(1));
    }

    #[test]
    fn idle_candidate_waits_for_persisted_timeout_even_when_backstop_elapsed() {
        let now = OffsetDateTime::now_utc();
        let candidate = super::idle_candidate_from_row(
            idle_row(
                json!({"idle_timeout_ms": 10_000}),
                now - TimeDuration::seconds(2),
            ),
            Duration::from_secs(1),
            now,
        )
        .unwrap();

        assert!(candidate.is_none());
    }

    #[test]
    fn idle_candidate_falls_back_to_backstop_for_missing_or_invalid_timeout() {
        let now = OffsetDateTime::now_utc();
        let candidate = super::idle_candidate_from_row(
            idle_row(
                json!({"idle_timeout_ms": "not-a-number"}),
                now - TimeDuration::seconds(2),
            ),
            Duration::from_secs(1),
            now,
        )
        .unwrap()
        .expect("candidate should use backstop");

        assert_eq!(candidate.idle_timeout, Duration::from_secs(1));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn sessions_round_trip_proxy_labels() {
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key = ThreadKey::parse(format!("test:proxy-labels-{}", Uuid::new_v4())).unwrap();
        let labels = BTreeMap::from([
            ("centaur.slack_user_id".to_owned(), "U123".to_owned()),
            ("centaur.slack_team_id".to_owned(), "T123".to_owned()),
            ("centaur.slack_channel_id".to_owned(), "C123".to_owned()),
        ]);

        let created = store
            .create_or_get_session(
                &thread_key,
                &HarnessType::Codex,
                None,
                json!({}),
                labels.clone(),
            )
            .await
            .expect("create session");

        assert_eq!(created.proxy_labels, labels);
        assert_eq!(
            store
                .get_session(&thread_key)
                .await
                .expect("get session")
                .proxy_labels,
            labels
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn create_or_get_session_merges_only_changed_metadata() {
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:metadata-merge-{}", Uuid::new_v4())).unwrap();
        let created = store
            .create_or_get_session_merging_metadata(
                &thread_key,
                &HarnessType::Codex,
                None,
                json!({
                    "mcp_tool_host": true,
                    "mcp_principal_id": "prn_test",
                    "console_user_name": "Old Name",
                }),
                Default::default(),
            )
            .await
            .expect("create session");

        let updated = store
            .create_or_get_session_merging_metadata(
                &thread_key,
                &HarnessType::Codex,
                None,
                json!({
                    "mcp_tool_host": true,
                    "mcp_principal_id": "prn_test",
                    "console_user_email": "test@example.com",
                    "console_user_name": "Test User",
                }),
                Default::default(),
            )
            .await
            .expect("merge session metadata");
        assert!(updated.updated_at >= created.updated_at);

        let unchanged = store
            .create_or_get_session_merging_metadata(
                &thread_key,
                &HarnessType::Codex,
                None,
                json!({
                    "mcp_tool_host": true,
                    "mcp_principal_id": "prn_test",
                    "console_user_email": "test@example.com",
                    "console_user_name": "Test User",
                }),
                Default::default(),
            )
            .await
            .expect("load session with unchanged metadata");
        assert_eq!(unchanged.updated_at, updated.updated_at);

        let metadata = sqlx::query_scalar::<_, serde_json::Value>(
            "select metadata from sessions where thread_key = $1",
        )
        .bind(thread_key.as_str())
        .fetch_one(store.pool())
        .await
        .expect("load session metadata");

        assert_eq!(
            metadata,
            json!({
                "mcp_tool_host": true,
                "mcp_principal_id": "prn_test",
                "console_user_email": "test@example.com",
                "console_user_name": "Test User",
            })
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn execution_requests_are_durable_and_idempotent() {
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key =
            ThreadKey::parse(format!("test:execution-request-{}", Uuid::new_v4())).unwrap();
        store
            .create_or_get_session(
                &thread_key,
                &HarnessType::Codex,
                None,
                json!({}),
                Default::default(),
            )
            .await
            .expect("create session");
        let first_request = json!({
            "idempotency_key": "slack-message-1",
            "input_lines": ["{\"type\":\"user\",\"text\":\"first\"}"],
            "metadata": {"source": "slackbotv2"},
            "idle_timeout_ms": 1000,
            "max_duration_ms": null
        });
        let first = store
            .create_execution_with_request(
                &thread_key,
                Some("slack-message-1"),
                json!({"source": "slackbotv2"}),
                first_request.clone(),
            )
            .await
            .expect("create execution with request");

        let replay = store
            .create_execution_with_request(
                &thread_key,
                Some("slack-message-1"),
                json!({"source": "different-replay"}),
                json!({"input_lines": ["different replay"]}),
            )
            .await
            .expect("replay idempotent execution request");

        assert!(first.created);
        assert!(!replay.created);
        assert_eq!(replay.execution.execution_id, first.execution.execution_id);
        store
            .set_execution_traceparent(
                &first.execution.execution_id,
                "00-0123456789abcdef0123456789abcdef-1111111111111111-01",
            )
            .await
            .expect("persist execution traceparent");
        assert_eq!(
            store
                .latest_execution_for_thread(&thread_key)
                .await
                .expect("load traced execution")
                .expect("execution exists")
                .metadata["centaur.traceparent"],
            "00-0123456789abcdef0123456789abcdef-1111111111111111-01"
        );
        assert_eq!(
            store
                .execution_request(&first.execution.execution_id)
                .await
                .expect("load persisted execution request"),
            first_request
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn idle_candidates_use_persisted_execution_idle_timeout() {
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key = ThreadKey::parse(format!("test:idle-cleanup-{}", Uuid::new_v4())).unwrap();
        let sandbox_id = format!("sbx-idle-{}", Uuid::new_v4());
        store
            .create_or_get_session(
                &thread_key,
                &HarnessType::Codex,
                None,
                json!({}),
                Default::default(),
            )
            .await
            .expect("create session");
        store
            .update_sandbox_id(&thread_key, Some(&sandbox_id))
            .await
            .expect("set sandbox id");
        let execution_id = store
            .create_execution(&thread_key, None, json!({"idle_timeout_ms": 1000}))
            .await
            .expect("create execution")
            .execution
            .execution_id;
        store
            .complete_execution(&execution_id)
            .await
            .expect("complete execution");
        sqlx::query(
            r#"
            update session_executions
            set completed_at = now() - interval '2 seconds', updated_at = now()
            where execution_id = $1
            "#,
        )
        .bind(&execution_id)
        .execute(store.pool())
        .await
        .expect("age execution");

        let candidates = store
            .list_idle_sandbox_candidates(Duration::from_secs(3600))
            .await
            .expect("list idle sandbox candidates");
        let candidate = candidates
            .iter()
            .find(|candidate| candidate.thread_key == thread_key)
            .expect("candidate should use execution idle timeout, not backstop");

        assert_eq!(candidate.sandbox_id, sandbox_id);
        assert_eq!(candidate.execution_id, execution_id);
        assert_eq!(candidate.idle_timeout, Duration::from_secs(1));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn stdout_owner_fences_output_and_terminal_updates() {
        let Some(store) = test_store().await else {
            return;
        };
        let thread_key = ThreadKey::parse(format!("test:stdout-owner-{}", Uuid::new_v4())).unwrap();
        store
            .create_or_get_session(
                &thread_key,
                &HarnessType::Codex,
                None,
                json!({}),
                Default::default(),
            )
            .await
            .expect("create session");
        let execution_id = store
            .create_execution(&thread_key, None, json!({}))
            .await
            .expect("create execution")
            .execution
            .execution_id;
        store
            .mark_execution_running(&execution_id)
            .await
            .expect("mark running");

        assert!(
            store
                .claim_stdout_owner(&execution_id, "owner-a", Duration::from_millis(25))
                .await
                .expect("owner-a claims stdout")
        );
        assert!(
            store
                .append_event_if_stdout_owner(
                    &thread_key,
                    &execution_id,
                    "owner-a",
                    Duration::from_millis(25),
                    "session.output.line",
                    json!("line-from-owner-a"),
                )
                .await
                .expect("owner-a appends")
                .is_some()
        );
        assert!(
            store
                .append_event_if_stdout_owner(
                    &thread_key,
                    &execution_id,
                    "owner-b",
                    Duration::from_millis(25),
                    "session.output.line",
                    json!("line-from-stale-owner-b"),
                )
                .await
                .expect("owner-b append is fenced")
                .is_none()
        );
        assert!(
            store
                .complete_execution_if_active_and_stdout_owner(&execution_id, "owner-b")
                .await
                .expect("owner-b terminal update is fenced")
                .is_none()
        );

        tokio::time::sleep(Duration::from_millis(40)).await;
        assert!(
            store
                .claim_expired_stdout_owner(&execution_id, "owner-b", Duration::from_secs(5))
                .await
                .expect("owner-b claims after lease expiry")
        );
        assert!(
            store
                .append_event_if_stdout_owner(
                    &thread_key,
                    &execution_id,
                    "owner-a",
                    Duration::from_secs(5),
                    "session.output.line",
                    json!("line-from-expired-owner-a"),
                )
                .await
                .expect("expired owner-a append is fenced")
                .is_none()
        );
        let completed = store
            .complete_execution_if_active_and_stdout_owner(&execution_id, "owner-b")
            .await
            .expect("owner-b completes")
            .expect("completion should be recorded");
        assert_eq!(
            completed.status,
            centaur_session_core::ExecutionStatus::Completed
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn releases_all_stdout_leases_held_by_one_owner() {
        let Some(store) = test_store().await else {
            return;
        };
        let owner = format!("owner-{}", Uuid::new_v4().simple());
        let peer = format!("peer-{}", Uuid::new_v4().simple());
        let mut owned = Vec::new();
        for label in ["a", "b"] {
            let thread_key =
                ThreadKey::parse(format!("test:handoff-{label}-{}", Uuid::new_v4())).unwrap();
            store
                .create_or_get_session(
                    &thread_key,
                    &HarnessType::Codex,
                    None,
                    json!({}),
                    Default::default(),
                )
                .await
                .expect("create session");
            let execution_id = store
                .create_execution(&thread_key, None, json!({}))
                .await
                .expect("create execution")
                .execution
                .execution_id;
            store
                .mark_execution_running(&execution_id)
                .await
                .expect("mark running");
            assert!(
                store
                    .claim_stdout_owner(&execution_id, &owner, Duration::from_secs(60))
                    .await
                    .expect("claim stdout owner")
            );
            owned.push((execution_id, thread_key));
        }
        // A bystander owner's lease must survive the release untouched.
        let bystander_thread =
            ThreadKey::parse(format!("test:handoff-bystander-{}", Uuid::new_v4())).unwrap();
        store
            .create_or_get_session(
                &bystander_thread,
                &HarnessType::Codex,
                None,
                json!({}),
                Default::default(),
            )
            .await
            .expect("create bystander session");
        let bystander_execution = store
            .create_execution(&bystander_thread, None, json!({}))
            .await
            .expect("create bystander execution")
            .execution
            .execution_id;
        store
            .mark_execution_running(&bystander_execution)
            .await
            .expect("mark bystander running");
        let bystander = format!("bystander-{}", Uuid::new_v4().simple());
        assert!(
            store
                .claim_stdout_owner(&bystander_execution, &bystander, Duration::from_secs(60))
                .await
                .expect("claim bystander lease")
        );
        assert_eq!(
            store
                .count_executions_with_stdout_owner(&owner)
                .await
                .expect("count owned"),
            2
        );

        let released = store
            .release_stdout_owned_executions(&owner)
            .await
            .expect("release owned leases");
        assert_eq!(released.len(), 2);
        for (execution_id, thread_key) in &owned {
            assert!(
                released.iter().any(|execution| {
                    execution.execution_id == *execution_id && execution.thread_key == *thread_key
                }),
                "released set must include {execution_id}"
            );
        }
        assert_eq!(
            store
                .count_executions_with_stdout_owner(&owner)
                .await
                .expect("count after release"),
            0
        );

        // Released leases are immediately claimable by a peer, without
        // waiting for expiry.
        assert!(
            store
                .claim_stdout_owner(&owned[0].0, &peer, Duration::from_secs(60))
                .await
                .expect("peer claims released lease")
        );

        assert_eq!(
            store
                .count_executions_with_stdout_owner(&bystander)
                .await
                .expect("count bystander"),
            1,
            "release must be scoped to the requested owner"
        );
        store
            .fail_execution_if_active(&bystander_execution, "test cleanup")
            .await
            .expect("terminalize bystander");

        // Terminal executions are never part of a release, even if a lease
        // column is still populated.
        for (execution_id, _) in &owned {
            store
                .fail_execution_if_active(execution_id, "test cleanup")
                .await
                .expect("terminalize execution");
        }
        assert!(
            store
                .release_stdout_owned_executions(&peer)
                .await
                .expect("release for peer")
                .is_empty()
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn warm_eviction_reservation_blocks_later_claims() {
        let Some(store) = test_store().await else {
            return;
        };
        let sandbox_id = format!("sbx-warm-evict-{}", Uuid::new_v4());
        let workload_key = format!("workload-warm-evict-{}", Uuid::new_v4());
        store
            .insert_ready_warm_sandbox(&sandbox_id, &workload_key)
            .await
            .expect("insert warm sandbox");
        sqlx::query(
            r#"
            update session_warm_sandboxes
            set created_at = now() - interval '100 years'
            where sandbox_id = $1
            "#,
        )
        .bind(&sandbox_id)
        .execute(store.pool())
        .await
        .expect("age warm sandbox");

        let reserved = store
            .reserve_ready_warm_sandboxes_for_eviction(1)
            .await
            .expect("reserve warm sandbox");

        assert_eq!(reserved, vec![sandbox_id.clone()]);
        assert_eq!(
            store
                .claim_ready_warm_sandbox(&workload_key, "test-thread")
                .await
                .expect("claim after reservation"),
            None
        );
        assert!(
            store
                .list_referenced_sandbox_ids()
                .await
                .expect("list referenced sandboxes")
                .contains(&sandbox_id)
        );

        store
            .mark_warm_sandbox_failed(&sandbox_id, "test cleanup")
            .await
            .expect("mark reserved warm sandbox failed");
        assert!(
            !store
                .list_referenced_sandbox_ids()
                .await
                .expect("list referenced sandboxes")
                .contains(&sandbox_id)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn normalized_rust_execution_events_write_only_aggregate_metrics() {
        let Some(store) = test_store().await else {
            return;
        };
        let scope = format!("metrics-test-{}", Uuid::new_v4().simple());
        let prior_scope = std::env::var(super::EXECUTION_METRICS_SCOPE_ENV).ok();
        unsafe { std::env::set_var(super::EXECUTION_METRICS_SCOPE_ENV, &scope) };
        let thread_key =
            ThreadKey::parse(format!("test:execution-metrics-{}", Uuid::new_v4())).unwrap();
        store
            .create_or_get_session(
                &thread_key,
                &HarnessType::Codex,
                Some("engineering"),
                json!({}),
                Default::default(),
            )
            .await
            .expect("create metrics session");
        let execution_id = store
            .create_execution(&thread_key, None, json!({}))
            .await
            .expect("create metrics execution")
            .execution
            .execution_id;
        store
            .mark_execution_running(&execution_id)
            .await
            .expect("mark metrics execution running");
        assert!(
            store
                .claim_stdout_owner(&execution_id, "metrics-owner", Duration::from_secs(5))
                .await
                .expect("claim metrics stdout")
        );

        let started = json!({
            "method": "item/started",
            "params": {
                "startedAtMs": 1_000,
                "item": {
                    "id": "metric-item",
                    "type": "commandExecution",
                    "command": "cargo build --release",
                    "cwd": "/private/repo",
                    "arguments": {"prompt": "private"}
                }
            }
        });
        let completed = json!({
            "method": "item/completed",
            "params": {
                "completedAtMs": 6_500,
                "item": {
                    "id": "metric-item",
                    "type": "commandExecution",
                    "status": "completed",
                    "output": "private output"
                },
                "unknown_future_key": "must not persist"
            }
        });
        for event in [&started, &completed, &completed] {
            store
                .append_event_if_stdout_owner(
                    &thread_key,
                    &execution_id,
                    "metrics-owner",
                    Duration::from_secs(5),
                    SESSION_OUTPUT_LINE_EVENT,
                    Value::String(event.to_string()),
                )
                .await
                .expect("append normalized event")
                .expect("owner appends normalized event");
        }

        let aggregate = sqlx::query_as::<_, (String, String, String, String, i64, i64)>(
            "select organization_scope, persona_key, language, command_family, sample_count, total_duration_ms from heartbeat_execution_metric_buckets where organization_scope = $1 and persona_key = 'engineering'",
        )
        .bind(&scope)
        .fetch_one(store.pool())
        .await
        .expect("load aggregate metric");
        assert_eq!(aggregate.0, scope);
        assert_eq!(aggregate.1, "engineering");
        assert_eq!(aggregate.2, "rust");
        assert_eq!(aggregate.3, "cargo_build");
        assert_eq!(aggregate.4, 1);
        assert_eq!(aggregate.5, 5_500);
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "select count(*) from information_schema.columns where table_name = 'heartbeat_execution_metric_buckets' and column_name in ('command', 'arguments', 'cwd', 'output', 'prompt', 'thread_key', 'execution_id', 'item_id')",
            )
            .fetch_one(store.pool())
            .await
            .expect("inspect aggregate schema"),
            0
        );

        let unknown_execution = store
            .create_execution(&thread_key, None, json!({}))
            .await
            .expect("create unknown execution")
            .execution
            .execution_id;
        store
            .mark_execution_running(&unknown_execution)
            .await
            .expect("mark unknown execution running");
        assert!(
            store
                .claim_stdout_owner(&unknown_execution, "metrics-owner", Duration::from_secs(5))
                .await
                .expect("claim unknown stdout")
        );
        let unknown = json!({
            "method": "item/completed",
            "params": {
                "completedAtMs": 2_000,
                "item": {"id": "unknown", "type": "commandExecution", "status": "completed", "command": "cargo metadata"}
            }
        });
        store
            .append_event_if_stdout_owner(
                &thread_key,
                &unknown_execution,
                "metrics-owner",
                Duration::from_secs(5),
                SESSION_OUTPUT_LINE_EVENT,
                Value::String(unknown.to_string()),
            )
            .await
            .expect("append unknown command")
            .expect("owner appends unknown command");
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "select count(*) from heartbeat_execution_metric_buckets where organization_scope = $1",
            )
            .bind(&scope)
            .fetch_one(store.pool())
            .await
            .expect("count aggregate metrics"),
            1
        );

        unsafe { std::env::remove_var(super::EXECUTION_METRICS_SCOPE_ENV) };
        let no_scope_execution = store
            .create_execution(&thread_key, None, json!({}))
            .await
            .expect("create no-scope execution")
            .execution
            .execution_id;
        store
            .mark_execution_running(&no_scope_execution)
            .await
            .expect("mark no-scope execution running");
        assert!(
            store
                .claim_stdout_owner(&no_scope_execution, "metrics-owner", Duration::from_secs(5))
                .await
                .expect("claim no-scope stdout")
        );
        let no_scope_started = json!({
            "method": "item/started",
            "params": {"startedAtMs": 1_000, "item": {"id": "no-scope", "type": "commandExecution", "command": "cargo check"}}
        });
        let no_scope_completed = json!({
            "method": "item/completed",
            "params": {"completedAtMs": 2_000, "item": {"id": "no-scope", "type": "commandExecution", "status": "completed"}}
        });
        for event in [&no_scope_started, &no_scope_completed] {
            store
                .append_event_if_stdout_owner(
                    &thread_key,
                    &no_scope_execution,
                    "metrics-owner",
                    Duration::from_secs(5),
                    SESSION_OUTPUT_LINE_EVENT,
                    Value::String(event.to_string()),
                )
                .await
                .expect("append no-scope event")
                .expect("owner appends no-scope event");
        }
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "select count(*) from heartbeat_execution_metric_buckets where organization_scope = $1",
            )
            .bind(&scope)
            .fetch_one(store.pool())
            .await
            .expect("count no-scope aggregate metrics"),
            1
        );

        unsafe { std::env::set_var(super::EXECUTION_METRICS_SCOPE_ENV, &scope) };
        let no_persona_thread = ThreadKey::parse(format!(
            "test:execution-metrics-no-persona-{}",
            Uuid::new_v4()
        ))
        .unwrap();
        store
            .create_or_get_session(
                &no_persona_thread,
                &HarnessType::Codex,
                None,
                json!({}),
                Default::default(),
            )
            .await
            .expect("create no-persona session");
        let no_persona_execution = store
            .create_execution(&no_persona_thread, None, json!({}))
            .await
            .expect("create no-persona execution")
            .execution
            .execution_id;
        store
            .mark_execution_running(&no_persona_execution)
            .await
            .expect("mark no-persona execution running");
        assert!(
            store
                .claim_stdout_owner(
                    &no_persona_execution,
                    "metrics-owner",
                    Duration::from_secs(5)
                )
                .await
                .expect("claim no-persona stdout")
        );
        let no_persona_started = json!({
            "method": "item/started",
            "params": {"startedAtMs": 1_000, "item": {"id": "no-persona", "type": "commandExecution", "command": "cargo check"}}
        });
        let no_persona_completed = json!({
            "method": "item/completed",
            "params": {"completedAtMs": 2_000, "item": {"id": "no-persona", "type": "commandExecution", "status": "completed"}}
        });
        for event in [&no_persona_started, &no_persona_completed] {
            store
                .append_event_if_stdout_owner(
                    &no_persona_thread,
                    &no_persona_execution,
                    "metrics-owner",
                    Duration::from_secs(5),
                    SESSION_OUTPUT_LINE_EVENT,
                    Value::String(event.to_string()),
                )
                .await
                .expect("append no-persona event")
                .expect("owner appends no-persona event");
        }
        assert_eq!(
            sqlx::query_scalar::<_, i64>(
                "select count(*) from heartbeat_execution_metric_buckets where organization_scope = $1",
            )
            .bind(&scope)
            .fetch_one(store.pool())
            .await
            .expect("count no-persona aggregate metrics"),
            1
        );

        sqlx::query("delete from heartbeat_execution_metric_buckets where organization_scope = $1")
            .bind(&scope)
            .execute(store.pool())
            .await
            .expect("cleanup aggregate metrics");
        sqlx::query("delete from sessions where thread_key = $1")
            .bind(thread_key.as_str())
            .execute(store.pool())
            .await
            .expect("cleanup metrics session");
        sqlx::query("delete from sessions where thread_key = $1")
            .bind(no_persona_thread.as_str())
            .execute(store.pool())
            .await
            .expect("cleanup no-persona session");
        match prior_scope {
            Some(value) => unsafe { std::env::set_var(super::EXECUTION_METRICS_SCOPE_ENV, value) },
            None => unsafe { std::env::remove_var(super::EXECUTION_METRICS_SCOPE_ENV) },
        }
    }

    #[test]
    fn command_metric_parser_accepts_only_allowlisted_rust_families() {
        assert_eq!(command_family("cargo build --release"), Some("cargo_build"));
        assert_eq!(
            command_family("/bin/bash -lc 'cargo clippy --all-targets'"),
            Some("cargo_clippy")
        );
        assert_eq!(command_family("rustc --version"), None);
        assert_eq!(command_family("rustc -V"), None);
        assert_eq!(command_family("rustc -Vv"), None);
        assert_eq!(command_family("rustc --help"), None);
        assert_eq!(command_family("cargo metadata"), None);
        assert_eq!(command_family("cargo build && curl example.invalid"), None);
    }

    #[test]
    fn command_metric_parser_ignores_unlisted_payload_keys() {
        let payload = Value::String(
            json!({
                "method": "item/completed",
                "params": {
                    "completedAtMs": 1_500,
                    "item": {
                        "id": "item-private",
                        "type": "commandExecution",
                        "status": "completed",
                        "command": "cargo test",
                        "cwd": "/private/repo",
                        "arguments": {"prompt": "private"},
                        "output": "private output",
                        "unknown_future_key": "must not affect metrics"
                    }
                }
            })
            .to_string(),
        );
        let parsed = parse_command_metric_event(&payload, true).expect("completed event");
        assert_eq!(parsed.item_id, "item-private");
        assert_eq!(parsed.timestamp_ms, 1_500);
        assert_eq!(parsed.command.as_deref(), Some("cargo test"));
        assert_eq!(parsed.status.as_deref(), Some("completed"));
        // The transient parser result is never serialized or inserted. The
        // durable row receives only the derived dimensions and timing bucket.
        assert_eq!(
            command_family(parsed.command.as_deref().unwrap()),
            Some("cargo_test")
        );
    }

    #[test]
    fn command_metric_parser_requires_protocol_timestamps_and_safe_dimensions() {
        let missing_timestamp = Value::String(
            json!({
                "method": "item/started",
                "params": {"item": {"id": "x", "type": "commandExecution", "command": "cargo test"}}
            })
            .to_string(),
        );
        assert!(parse_command_metric_event(&missing_timestamp, false).is_none());
        assert_eq!(
            safe_dimension("engineering"),
            Some("engineering".to_owned())
        );
        assert_eq!(safe_dimension("/private/repo"), None);
        assert_eq!(safe_dimension(""), None);
    }
}
