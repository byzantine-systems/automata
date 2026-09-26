-- What a correction command corrects, written in the submission's own
-- transaction so a correction without its details cannot exist.
INSERT INTO fsm_command_correction (command_id, effective_at, on_divergence, replay_limit)
    VALUES (@command_id, @effective_at, @on_divergence, @replay_limit);
