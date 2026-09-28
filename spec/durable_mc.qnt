// Model-checking instances of the durable protocol.
//   durable_mc        : the code as written (4866070): READ COMMITTED T-W1, children, crashes, DRIFT=0
//   durable_mc_drift  : same, local clock lags DB time by 2 ticks (violates S13's clock assumption)
//   durable_mc_env    : durable_mc plus external writes (EnvCorruptActivityBounds, invalid-bounds commands): G10 quarantine
//   durable_mc_act    : activity-only (no children, no crashes, 4 workflows, 2 activities), READ COMMITTED.
//                       4 workflows and clock bound 10 keep a 40-step sample from exhausting the instance:
//                       with 2 and 6 most samples ran out of work within ~13 steps and spent the rest
//                       in states where almost every branch of `step` is disabled (~2 samples/s, not ~25)
//   durable_mc_rr     : HISTORICAL (pre-P4, 0085691): T-W1 plain reads use a REPEATABLE READ snapshot
//   durable_mc_act_rr : HISTORICAL activity-only instance with the RR snapshot
//   durable_mc_d3     : Apalache only (verify.sh, --max-steps <= 3): durable_mc with MAX_WF = 3 and
//                       MAX_ACT = 1. Up to depth 3 it reaches the same states as durable_mc, less the
//                       ids no 3-step run can create (the reach bound in spec/README.md, "Apalache").
//                       Not for simulation: 40-step samples need the larger bounds.
module durable_mc {
  import durable(
    RUNTIMES = Set(1, 2), MAX_WF = 4, MAX_ACT = 3, TOPICS = Set("t"), TOPIC_CAP = Map("t" -> 1),
    LOCAL_SLOTS = 1, MAX_ATTEMPTS = 2, MAX_ACTIVATION = 2, LEASE_W = 3, LEASE_A = 3, MAX_TIME = 10,
    DRIFT = 0, RR_SNAPSHOT = false, ENABLE_CHILDREN = true, ENABLE_CRASH = true, ENABLE_ENV_EDITS = false, MAX_TASK_RESTARTS = 1
  ).* from "./durable"
}

module durable_mc_drift {
  import durable(
    RUNTIMES = Set(1, 2), MAX_WF = 4, MAX_ACT = 3, TOPICS = Set("t"), TOPIC_CAP = Map("t" -> 1),
    LOCAL_SLOTS = 1, MAX_ATTEMPTS = 2, MAX_ACTIVATION = 2, LEASE_W = 3, LEASE_A = 3, MAX_TIME = 10,
    DRIFT = 2, RR_SNAPSHOT = false, ENABLE_CHILDREN = true, ENABLE_CRASH = true, ENABLE_ENV_EDITS = false, MAX_TASK_RESTARTS = 1
  ).* from "./durable"
}

module durable_mc_env {
  import durable(
    RUNTIMES = Set(1, 2), MAX_WF = 4, MAX_ACT = 3, TOPICS = Set("t"), TOPIC_CAP = Map("t" -> 1),
    LOCAL_SLOTS = 1, MAX_ATTEMPTS = 2, MAX_ACTIVATION = 2, LEASE_W = 3, LEASE_A = 3, MAX_TIME = 10,
    DRIFT = 0, RR_SNAPSHOT = false, ENABLE_CHILDREN = true, ENABLE_CRASH = true, ENABLE_ENV_EDITS = true, MAX_TASK_RESTARTS = 1
  ).* from "./durable"
}

module durable_mc_act {
  import durable(
    RUNTIMES = Set(1, 2), MAX_WF = 4, MAX_ACT = 2, TOPICS = Set("t"), TOPIC_CAP = Map("t" -> 1),
    LOCAL_SLOTS = 1, MAX_ATTEMPTS = 2, MAX_ACTIVATION = 2, LEASE_W = 3, LEASE_A = 3, MAX_TIME = 10,
    DRIFT = 0, RR_SNAPSHOT = false, ENABLE_CHILDREN = false, ENABLE_CRASH = false, ENABLE_ENV_EDITS = false, MAX_TASK_RESTARTS = 1
  ).* from "./durable"
}

module durable_mc_rr {
  import durable(
    RUNTIMES = Set(1, 2), MAX_WF = 4, MAX_ACT = 3, TOPICS = Set("t"), TOPIC_CAP = Map("t" -> 1),
    LOCAL_SLOTS = 1, MAX_ATTEMPTS = 2, MAX_ACTIVATION = 2, LEASE_W = 3, LEASE_A = 3, MAX_TIME = 10,
    DRIFT = 0, RR_SNAPSHOT = true, ENABLE_CHILDREN = true, ENABLE_CRASH = true, ENABLE_ENV_EDITS = false, MAX_TASK_RESTARTS = 1
  ).* from "./durable"
}

module durable_mc_act_rr {
  import durable(
    RUNTIMES = Set(1, 2), MAX_WF = 4, MAX_ACT = 2, TOPICS = Set("t"), TOPIC_CAP = Map("t" -> 1),
    LOCAL_SLOTS = 1, MAX_ATTEMPTS = 2, MAX_ACTIVATION = 2, LEASE_W = 3, LEASE_A = 3, MAX_TIME = 10,
    DRIFT = 0, RR_SNAPSHOT = true, ENABLE_CHILDREN = false, ENABLE_CRASH = false, ENABLE_ENV_EDITS = false, MAX_TASK_RESTARTS = 1
  ).* from "./durable"
}

module durable_mc_d3 {
  import durable(
    RUNTIMES = Set(1, 2), MAX_WF = 3, MAX_ACT = 1, TOPICS = Set("t"), TOPIC_CAP = Map("t" -> 1),
    LOCAL_SLOTS = 1, MAX_ATTEMPTS = 2, MAX_ACTIVATION = 2, LEASE_W = 3, LEASE_A = 3, MAX_TIME = 10,
    DRIFT = 0, RR_SNAPSHOT = false, ENABLE_CHILDREN = true, ENABLE_CRASH = true, ENABLE_ENV_EDITS = false, MAX_TASK_RESTARTS = 1
  ).* from "./durable"
}
