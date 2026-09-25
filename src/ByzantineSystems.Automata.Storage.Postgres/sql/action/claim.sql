-- Leases up to @batch actions from one machine's queue.
--
-- read_ct is the fencing token every later call carries. It is pgmq's own
-- delivery counter, which every read increments, so the value returned here is
-- stale the instant another worker reads the same message. That is exactly the
-- property a fence needs.
--
-- The column list is explicit, as everywhere else: this result is the contract
-- between the routine and a reader that maps by name.
SELECT
    a.msg_id,
    a.read_ct,
    a.vt,
    a.message
FROM
    fsm.claim_actions (@queue, @batch, @lease) a;

