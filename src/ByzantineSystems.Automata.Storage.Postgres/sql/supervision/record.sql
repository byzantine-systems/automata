-- Appends one supervision audit fact. Append-only by design: a supervision
-- record is evidence of a decision already taken.
INSERT INTO fsm.supervision_event (supervisor, child_id, kind, strategy, reason, at)
    VALUES (@supervisor, @child_id, @kind, @strategy, @reason::jsonb, @at);

