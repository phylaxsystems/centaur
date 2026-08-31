-- Privacy-safe Rust build/check/test command-time metrics.  These timings are
-- not compile-only: tests, documentation, and benchmarks may include other
-- work.  The writer derives an allowlisted metric
-- from normalized command-execution events before inserting here.  Raw event
-- payloads and identifiers are never copied into this relation.
create table if not exists heartbeat_execution_metric_buckets (
    organization_scope text not null,
    persona_key text not null,
    language text not null,
    command_family text not null,
    bucket_start timestamptz not null,
    duration_bucket_ms integer not null,
    status text not null,
    sample_count bigint not null default 0,
    total_duration_ms bigint not null default 0,
    constraint heartbeat_execution_metric_scope_check
        check (organization_scope ~ '^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$'),
    constraint heartbeat_execution_metric_persona_check
        check (persona_key ~ '^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$'),
    constraint heartbeat_execution_metric_language_check
        check (language = 'rust'),
    constraint heartbeat_execution_metric_family_check
        check (command_family in ('cargo_build', 'cargo_check', 'cargo_test',
                                  'cargo_clippy', 'cargo_doc', 'cargo_bench', 'rustc')),
    constraint heartbeat_execution_metric_duration_check
        check (duration_bucket_ms in (100, 250, 500, 1000, 2500, 5000, 10000,
                                      30000, 60000, 300000, 900000, 3600000,
                                      86400000)),
    constraint heartbeat_execution_metric_bucket_alignment_check
        check (bucket_start = date_trunc('hour', bucket_start at time zone 'UTC')
                                at time zone 'UTC'),
    constraint heartbeat_execution_metric_status_check
        check (status in ('completed', 'failed')),
    constraint heartbeat_execution_metric_counts_check
        check (sample_count > 0 and total_duration_ms >= 0),
    primary key (organization_scope, persona_key, language, command_family,
                 bucket_start, duration_bucket_ms, status)
);

create index if not exists heartbeat_execution_metric_window_idx
    on heartbeat_execution_metric_buckets (organization_scope, bucket_start,
                                           command_family, persona_key);

revoke all on heartbeat_execution_metric_buckets from public;
grant insert on heartbeat_execution_metric_buckets to current_user;

do $$
begin
    if exists (select 1 from pg_roles where rolname = 'centaur_heartbeat_definer') then
        grant select, delete on heartbeat_execution_metric_buckets
            to centaur_heartbeat_definer;
    end if;
end
$$;

create or replace function heartbeat_aggregate_execution_metrics(
    p_profile_id uuid,
    p_window_start timestamptz,
    p_window_end timestamptz,
    p_limit integer
)
returns table (
    organization_scope text,
    persona_key text,
    language text,
    command_family text,
    status text,
    sample_count bigint,
    total_duration_ms bigint,
    p50_duration_bucket_ms integer,
    p95_duration_bucket_ms integer,
    percentiles_approximate boolean
)
language plpgsql
security definer
set search_path = pg_catalog, public
as $$
declare
    p record;
begin
    if heartbeat_current_workflow_principal() <> 'workflow-heartbeat-run' then
        raise exception 'execution metrics require the heartbeat run workflow'
            using errcode = '42501';
    end if;
    if p_window_start is null or p_window_end is null
       or p_window_start >= p_window_end
       or p_window_end - p_window_start > interval '14 days'
       or date_trunc('hour', p_window_start at time zone 'UTC') at time zone 'UTC'
             <> p_window_start
       or date_trunc('hour', p_window_end at time zone 'UTC') at time zone 'UTC'
             <> p_window_end then
        raise exception 'execution metric window must be positive, at most 14 days, and UTC-hour aligned'
            using errcode = '22023';
    end if;
    if p_limit is null or p_limit < 1 or p_limit > 25 then
        raise exception 'execution metric limit must be between 1 and 25'
            using errcode = '22023';
    end if;

    select * into p
     from public.heartbeat_profiles
     where profile_id = p_profile_id
       and scope_kind = 'organization'
       and workflow_name = 'heartbeat_run'
       and executor_principal_foreign_id = heartbeat_current_workflow_principal();
    if not found then
        raise exception 'workflow principal does not operate this organization profile'
            using errcode = '42501';
    end if;

    return query
    with bucket_rows as (
        select m.organization_scope, m.persona_key, m.language,
               m.command_family, m.status, m.duration_bucket_ms,
               m.sample_count, m.total_duration_ms
          from public.heartbeat_execution_metric_buckets m
         where m.organization_scope = p.scope_ref
           and m.bucket_start >= p_window_start
           and m.bucket_start < p_window_end
    ), grouped as (
        select b.organization_scope, b.persona_key, b.language,
               b.command_family, b.status,
               sum(b.sample_count)::bigint as sample_count,
               sum(b.total_duration_ms)::bigint as total_duration_ms
          from bucket_rows b
         group by b.organization_scope, b.persona_key, b.language,
                  b.command_family, b.status
        having sum(b.sample_count) >= 3
    ), ranked as (
        select b.*, g.sample_count as group_sample_count,
               sum(b.sample_count) over (
                   partition by b.organization_scope, b.persona_key, b.language,
                                b.command_family, b.status
                   order by b.duration_bucket_ms
               ) as cumulative_count
          from bucket_rows b
          join grouped g using (organization_scope, persona_key, language,
                                command_family, status)
    ), percentiles as (
        select r.organization_scope, r.persona_key, r.language,
               r.command_family, r.status,
               min(r.duration_bucket_ms) filter (
                   where r.cumulative_count >= ceil(r.group_sample_count * 0.50)
               ) as p50_duration_bucket_ms,
               min(r.duration_bucket_ms) filter (
                   where r.cumulative_count >= ceil(r.group_sample_count * 0.95)
               ) as p95_duration_bucket_ms
          from ranked r
         group by r.organization_scope, r.persona_key, r.language,
                  r.command_family, r.status
    )
    select g.organization_scope, g.persona_key, g.language,
           g.command_family, g.status, g.sample_count, g.total_duration_ms,
           p.p50_duration_bucket_ms, p.p95_duration_bucket_ms, true
      from grouped g
      join percentiles p using (organization_scope, persona_key, language,
                                command_family, status)
     order by g.total_duration_ms desc, g.command_family, g.persona_key,
              g.status
     limit p_limit;
end;
$$;

alter function heartbeat_aggregate_execution_metrics(uuid, timestamptz, timestamptz, integer)
    owner to centaur_heartbeat_definer;
revoke all on function heartbeat_aggregate_execution_metrics(uuid, timestamptz, timestamptz, integer)
    from public;
grant execute on function heartbeat_aggregate_execution_metrics(uuid, timestamptz, timestamptz, integer)
    to centaur_heartbeat_run;

-- Metrics are deployment-wide rather than profile-owned.  Keep a fixed
-- retention boundary and expose cleanup as a separate operation so the
-- existing heartbeat retention contract remains unchanged.
create or replace function heartbeat_apply_execution_metric_retention(p_profile_id uuid)
returns jsonb
language plpgsql
security definer
set search_path = pg_catalog, public
as $$
declare
    p record;
    metric_count integer := 0;
begin
    if heartbeat_current_workflow_principal() <> 'workflow-heartbeat-run' then
        raise exception 'execution metric retention requires the heartbeat run workflow' using errcode = '42501';
    end if;
    select * into p from public.heartbeat_profiles
     where profile_id = p_profile_id
       and scope_kind = 'organization'
       and workflow_name = 'heartbeat_run'
       and executor_principal_foreign_id = heartbeat_current_workflow_principal();
    if not found then
        raise exception 'workflow principal does not operate this organization profile' using errcode = '42501';
    end if;

    delete from public.heartbeat_execution_metric_buckets
     where organization_scope = p.scope_ref
       and bucket_start < now() - interval '90 days';
    get diagnostics metric_count = row_count;

    return jsonb_build_object('execution_metric_buckets_deleted', metric_count);
end;
$$;
alter function heartbeat_apply_execution_metric_retention(uuid) owner to centaur_heartbeat_definer;
revoke all on function heartbeat_apply_execution_metric_retention(uuid) from public;
grant execute on function heartbeat_apply_execution_metric_retention(uuid) to centaur_heartbeat_run;
