-- Treat the ETL channel catalog's syncable bit as an authorization boundary.
-- Excluded, inaccessible, archived, or otherwise out-of-scope channels must
-- not remain readable through stale normalized rows or derived documents.

drop policy if exists centaur_slack_channels_reader_select
    on slack_sync_channels;
create policy centaur_slack_channels_reader_select
    on slack_sync_channels
    for select
    to centaur_slack_reader
    using (
        is_syncable
        and channel_id = centaur_current_slack_channel_id()
    );

drop policy if exists centaur_slack_messages_reader_select
    on slack_sync_messages;
create policy centaur_slack_messages_reader_select
    on slack_sync_messages
    for select
    to centaur_slack_reader
    using (
        exists (
            select 1
            from slack_sync_channels channels
            where channels.channel_id = slack_sync_messages.channel_id
        )
    );

drop policy if exists centaur_slack_message_attachments_reader_select
    on slack_sync_message_attachments;
create policy centaur_slack_message_attachments_reader_select
    on slack_sync_message_attachments
    for select
    to centaur_slack_reader
    using (
        exists (
            select 1
            from slack_sync_channels channels
            where channels.channel_id = slack_sync_message_attachments.channel_id
        )
    );

drop policy if exists centaur_context_docs_reader_select
    on company_context_documents;
create policy centaur_context_docs_reader_select
    on company_context_documents
    for select
    to centaur_slack_reader
    using (
        source = 'slack'
        and exists (
            select 1
            from slack_sync_channels channels
            where channels.channel_id = metadata ->> 'channel_id'
        )
    );

drop policy if exists centaur_readonly_slack_sync_channels_select
    on slack_sync_channels;
create policy centaur_readonly_slack_sync_channels_select
    on slack_sync_channels
    for select
    to centaur_readonly
    using (
        is_syncable
        and (
            not is_private
            or channel_id = centaur_current_slack_channel_id()
        )
    );

drop policy if exists centaur_cc_reader_channels_select
    on slack_sync_channels;
create policy centaur_cc_reader_channels_select
    on slack_sync_channels
    for select
    to centaur_company_context_reader
    using (
        is_syncable
        and (
            channel_id = centaur_current_slack_channel_id()
            or channel_id = any(
                (select centaur_current_slack_history_channel_ids())::text[]
            )
            or (
                (select centaur_company_context_include_public_slack())
                and not is_private
            )
            or (
                is_private
                and centaur_can_read_slack_user_conversation(
                    centaur_current_slack_team_id(),
                    channel_id
                )
            )
        )
    );
