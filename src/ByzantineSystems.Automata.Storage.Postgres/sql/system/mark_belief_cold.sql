-- Records the cutoff the refresh just used, so the next pass knows it has
-- nothing to do until the day turns. See fsm.mark_belief_cold_refreshed.
SELECT
    fsm.mark_belief_cold_refreshed () AS refreshed_for;

