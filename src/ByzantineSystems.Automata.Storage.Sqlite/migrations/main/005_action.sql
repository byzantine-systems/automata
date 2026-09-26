-- ---------------------------------------------------------------------------
-- fsm_action: the effects a commit asked for, waiting to be delivered.
--
-- The PostgreSQL store hands these to pgmq. Here they are an ordinary table
-- written by the commit's own transaction, which keeps the property that
-- matters: "the transition happened and its effects were queued" is one fact,
-- and neither half can exist without the other.
--
-- Identity is (command_id, ordinal), the pair a destination deduplicates on.
-- There is no separate message id, because nothing here needs one.
--
-- Delivery is leased and fenced exactly like the inbox. A claim hands out a
-- fresh token from fsm_counter and increments read_ct; completing,
-- rescheduling or abandoning requires both to still match, so a worker whose
-- lease lapsed and was taken over cannot act on the delivery.
--
-- A completed action is deleted. Delivered actions are not audit data: the
-- transition that asked for them is, and it stays in fsm_transition.
-- ---------------------------------------------------------------------------
CREATE TABLE fsm_action (
    command_id INTEGER NOT NULL,
    ordinal INTEGER NOT NULL,
    machine_id TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    epoch INTEGER NOT NULL,
    action TEXT NOT NULL,
    -- Earliest instant it may be claimed while unleased; the lease deadline
    -- while leased. An expired lease makes it claimable again on its own.
    visible_at INTEGER NOT NULL,
    lease_token INTEGER NOT NULL DEFAULT 0,
    read_ct INTEGER NOT NULL DEFAULT 0,
    enqueued_at INTEGER NOT NULL,
    CONSTRAINT action_pkey PRIMARY KEY (command_id, ordinal),
    CONSTRAINT action_ordinal_not_negative CHECK (ordinal >= 0),
    CONSTRAINT action_epoch_positive CHECK (epoch > 0),
    CONSTRAINT action_is_json CHECK (JSON_VALID(action)),
    CONSTRAINT action_counters_non_negative CHECK (lease_token >= 0 AND read_ct >= 0),
    CONSTRAINT action_command_fkey FOREIGN KEY (command_id) REFERENCES fsm_command (command_id)
) STRICT;

-- The claim path: the oldest visible actions of one machine. Unordered by
-- design beyond that, as the contract says, so there is no entity in it.
CREATE INDEX fsm_action_claim_idx ON fsm_action (machine_id, visible_at);

-- ---------------------------------------------------------------------------
-- fsm_action_dead_letter: the effects that never happened.
--
-- The business fact under an abandoned delivery: the machine decided an action
-- should happen, the application could not make it happen, and we stopped
-- trying. Kept alongside the transition that asked for it.
-- ---------------------------------------------------------------------------
CREATE TABLE fsm_action_dead_letter (
    machine_id TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    command_id INTEGER NOT NULL,
    epoch INTEGER NOT NULL,
    ordinal INTEGER NOT NULL,
    action TEXT NOT NULL,
    -- Written by the library, never by the application.
    reason TEXT NOT NULL,
    deliveries INTEGER NOT NULL,
    recorded_at INTEGER NOT NULL,
    CONSTRAINT action_dead_letter_pkey PRIMARY KEY (command_id, ordinal),
    CONSTRAINT action_dead_letter_ordinal_not_negative CHECK (ordinal >= 0),
    CONSTRAINT action_dead_letter_deliveries_positive CHECK (deliveries > 0),
    CONSTRAINT action_dead_letter_reason_not_blank CHECK (LENGTH(TRIM(reason)) > 0),
    CONSTRAINT action_dead_letter_is_json CHECK (JSON_VALID(action)),
    CONSTRAINT action_dead_letter_command_fkey FOREIGN KEY (command_id) REFERENCES fsm_command (command_id)
) STRICT;

-- "What has this machine failed to deliver lately".
CREATE INDEX fsm_action_dead_letter_machine_idx ON fsm_action_dead_letter (machine_id, recorded_at DESC);
