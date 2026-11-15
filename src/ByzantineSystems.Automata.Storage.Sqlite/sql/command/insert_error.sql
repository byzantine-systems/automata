-- One header and exactly its selected child, inside the finalization transaction.
INSERT INTO fsm_command_error (command_id, kind, recorded_at) VALUES (@command_id, @kind, @now);
INSERT INTO fsm_command_error_domain(command_id, error)
    SELECT @command_id, @domain WHERE @kind = 'domain';
INSERT INTO fsm_command_error_machine(command_id, reason)
    SELECT @command_id, @reason WHERE @kind = 'machine';
INSERT INTO fsm_command_error_replay(command_id, replay_kind, version, epoch, reason, budget)
    SELECT @command_id, @replay_kind, @version, @failure_epoch, @reason, @budget WHERE @kind = 'replay';
