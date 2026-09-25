// Model-checking instances of the durable protocol.
//   durable_mc        : the code as written (4866070): READ COMMITTED T-W1, children, crashes, DRIFT=0
//   durable_mc_drift  : same, local clock lags DB time by 2 ticks (violates S13's clock assumption)
//   durable_mc_env    : durable_mc plus external writes (EnvCorruptActivityBounds, invalid-bounds commands): G10
//   durable_mc_act    : activity-only (no children, no crashes, 2 workflows, 2 activities), READ COMMITTED
//   durable_mc_rr     : HISTORICAL (pre-P4, 0085691): T-W1 plain reads use a REPEATABLE READ snapshot
//   durable_mc_act_rr : HISTORICAL activity-only instance with the RR snapshot
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
    RUNTIMES = Set(1, 2), MAX_WF = 2, MAX_ACT = 2, TOPICS = Set("t"), TOPIC_CAP = Map("t" -> 1),
    LOCAL_SLOTS = 1, MAX_ATTEMPTS = 2, MAX_ACTIVATION = 2, LEASE_W = 3, LEASE_A = 3, MAX_TIME = 6,
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
    RUNTIMES = Set(1, 2), MAX_WF = 2, MAX_ACT = 2, TOPICS = Set("t"), TOPIC_CAP = Map("t" -> 1),
    LOCAL_SLOTS = 1, MAX_ATTEMPTS = 2, MAX_ACTIVATION = 2, LEASE_W = 3, LEASE_A = 3, MAX_TIME = 6,
    DRIFT = 0, RR_SNAPSHOT = true, ENABLE_CHILDREN = false, ENABLE_CRASH = false, ENABLE_ENV_EDITS = false, MAX_TASK_RESTARTS = 1
  ).* from "./durable"
}
