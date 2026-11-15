-- Appends one supervision audit fact. Append-only by design: a supervision
-- record is evidence of a decision already taken.
INSERT INTO $schema$.supervision_event (supervisor, child_id, kind, strategy, reason, at)
    VALUES (@supervisor, @child_id, @kind::$schema$.supervision_kind, @strategy::$schema$.supervision_strategy, @reason, @at);

