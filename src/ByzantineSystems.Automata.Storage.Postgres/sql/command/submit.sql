-- Submits a command to the inbox.
--
-- Named argument notation is used deliberately. fsm.submit_command takes twelve
-- parameters, seven of which are optional audit context, and positional
-- notation would make a reordering during a future edit a silent bug rather
-- than a compile error.
--
-- accepted = false means this idempotency key was already submitted, and
-- command_id identifies the original. That is a normal outcome, not a failure:
-- it is how a client retry after a lost response is recognised.
SELECT
    s.command_id,
    s.seq,
    s.accepted
FROM
    fsm.submit_command (p_machine_id => @machine_id, p_entity_id => @entity_id, p_idempotency_key => @idempotency_key, p_chart_version => @chart_version, p_event => @event::jsonb, p_visible_at => @visible_at::timestamptz, p_received_at => @received_at::timestamptz, p_tenant => @tenant, p_principal => @principal, p_source => @source, p_correlation_id => @correlation_id, p_causation_id => @causation_id) s;

