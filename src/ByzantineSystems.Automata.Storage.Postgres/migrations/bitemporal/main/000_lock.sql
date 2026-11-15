-- Held through the complete temporal DbUp transaction, including the seed and profile flip.
LOCK TABLE $schema$.store_profile IN EXCLUSIVE MODE;

LOCK TABLE $schema$.entity_state IN SHARE MODE;

