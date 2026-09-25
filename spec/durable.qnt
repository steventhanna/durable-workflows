// -*- mode: Bluespec; -*-
/**
 * Quint model of the durable-workflows core protocol (second iteration).
 *
 * Source of truth: docs/INVARIANTS.md and the cited code in
 * durable-workflows/src/ at commit 4866070 (after P4: every library
 * transaction runs at READ COMMITTED, src/dialect/{mysql,postgres}.rs
 * `transaction`). The model mirrors the CODE, including its fences.
 *
 * Granularity: one Quint action per committed database transaction, named
 * after its T-id. A transaction that loses a fence rolls back; the model
 * shows that as a separate "fence miss" action (or a no-op branch) that only
 * changes process state. The exception is T-W1 (claim_batch / claim_one): it
 * holds the topic lock across many statements, so it is split into one action
 * per group of statements, with the interleaving points that exist under
 * READ COMMITTED (each plain read sees what was committed when that statement
 * started; locking reads and UPDATEs are current reads). T-W1's own writes
 * are buffered in proc.tw1 (invisible to others until TW1_Commit) and the
 * rows it has locked block every other transaction that would lock them.
 *
 * Action -> transaction map (file:function). Every action takes all its choices as
 * parameters (trace-checking interface version TRACE_IFACE_VERSION; see spec/README.md).
 *   TX1_Start(wNew, kind, key, from, inserted, tnow)  T-X1 store.rs start_with_conn -> insert_prepared ->
 *                                 persistence/workflows.rs insert_started -> dialect/mysql.rs insert_workflow
 *   TX2_RecoverableStart(kind, key, orig, latest, superseded, sNew, tnow)  T-X2 store.rs start_or_restart_recoverable_with_conn
 *   TX3_Cancel(w, tnow)           T-X3 store.rs cancel_with_conn / cancel_locked_workflow / cancel_activities
 *   TC1_Claim(r, rec, cl, tok, leaseExp, tnow)  T-C1 runtime/coordinator.rs claim_one (recovery + ready claim)
 *   LC1_NoEvent(r, w)             L-C1 coordinator.rs activate_claim_inner (no deliverable event -> task error)
 *   TC2_Continue / TC2_Complete / TC2_RunActivity / TC2_RunChild   T-C2 coordinator.rs commit_on_connection,
 *                                 commit_wait_transition, commit_activity, commit_child
 *   TC3_ActivationFailure(r, w, tok, attempt, maxActivation, availableAt, tnow)  T-C3 coordinator.rs record_activation_failure
 *   CoordFenceMiss(r, w, tok)     T-C2/T-C3 fence miss -> FencedWrite -> task error (G1)
 *   T-W1 (activity_worker.rs claim_batch / claim_one), simulation form, one action per statement group:
 *     TW1_BeginBatch(r)           topic rows FOR UPDATE SKIP LOCKED (all or nothing), then now
 *     TW1_BeginOne(r, t, tnow)    claim_one: now sampled BEFORE the blocking topic FOR UPDATE
 *     TW1_ReconcileScan(r, t)     reconcile_expired: plain candidate read (running, lease <= now)
 *     TW1_ReconcileRow(r, a, availableAt)  reconcile_expired: workflow FOR UPDATE, activity FOR UPDATE + recheck,
 *                                 updates (pending at availableAt = now + retry delay, or dead-lettered at now)
 *     TW1_Count(r)                in_flight count + candidate join (two plain reads)
 *     TW1_ClaimRow(r, a, tok)     claim_locked_candidate: workflow + activity FOR UPDATE SKIP LOCKED + recheck, claim
 *     TW1_SkipRow(r, a)           claim_locked_candidate returns None (recheck fails or SKIP LOCKED)
 *     TW1_Commit(r)               COMMIT
 *   T-W1 replay form: TW1_Claim(r, tnow, localAvail, reconciled, inFlightSeen, claimed) (one committed T-W1)
 *   TW1_Error(r, a, reason)       T-W1 claim_locked_candidate error on row a (attempt_cap | invalid_bounds)
 *                                 -> the whole T-W1 rolls back, task error (G10)
 *   EnvCorruptActivityBounds(a)   environment: an external write (raw SQL, serde-built command) leaves
 *                                 activity a with invalid timeout/lease bounds (ENABLE_ENV_EDITS)
 *   EnvSetWf(w, row) / EnvSetAct(a, row) / EnvSetAtt(a, n, row) / EnvAppendEvent(w, ev)
 *                                 environment (trace replay only, not in step): a recorded external
 *                                 write sets one durable row / appends one deliverable event (ENABLE_ENV_EDITS)
 *   TW2_Send(r, a, tok, hb) / TW2_Commit(hb, sample, leaseExp) / TW2_FenceMiss(hb) / TW2_Drop(hb)
 *                                 T-W2 activity_worker.rs heartbeat_once / heartbeat_loop
 *   HandlerReturn(r, a, tok, outcome)  L-W handler returns / times out (local step)
 *   LocalDeadline(r, a, tok)      L-W activity_worker.rs execute_claim: local lease deadline -> no T-W3
 *   TW3_Finish(r, a, tok, outcome, availableAt, tnow)  T-W3 activity_worker.rs finish_on_connection
 *   TW3_FenceMiss(r, a, tok)      T-W3 fence miss or missing wait -> rollback
 *   AdminPause(w, tnow)           T-A2 admin/control.rs pause_workflow (+ pause_activity: S36)
 *   AdminResume(w, tnow)          T-A3 admin/control.rs resume_workflow / resume_status
 *   AdminCancel(w, tnow)          T-A4 admin/control.rs cancel_workflow -> store.rs cancel_locked_workflow
 *   Crash(r)                      process dies: claims, executions, in-flight T-W1 lost (INVARIANTS §5.3)
 *   Tick                          simulation only: DB time advances by one unit
 *
 * Time: every DB action takes tnow (the DB time it observed) and sets now' = max(now, tnow).
 * Identifiers: workflow/activity ids and lease tokens are counters (db.nextWf, db.nextAct,
 * db.nextToken): the n-th insert / generated token gets n. Heartbeat ids: ghost.nextHb.
 * Observed values (lease expiries, availableAt, maxAttempts) are parameters, not recomputed.
 *
 * Invariant -> S/G id map (see the bottom of this file)
 *   S1  inv_S1_wfLease            S2  inv_S2_noStaleWfCommit   S3  wit_S3_concurrentStep (witness)
 *   S5  inv_S5_delivery           S6  inv_S6_wfTerminalAbsorbing
 *   S7  inv_S7_waitCoupling       S8  inv_S8_activationBounded
 *   S9  inv_S9_actLease           S10 inv_S10_noStaleActCommit S11 inv_S11_openAttempt
 *   S12 inv_S12_attemptsBounded   S13 inv_S13_oneHandler, inv_S13_topicConcurrency
 *   S14 inv_S14_actTerminal       S15 inv_S15_actWfCoupling    S16 inv_S16_successOnce
 *   S17 inv_S17_capAtClaim (as stated), inv_S17_capAlways (between commits)
 *   S18 inv_S18_dedup             S19 inv_S19_successor, inv_S19_sourceTerminal   S23 inv_S23_childOnce
 *   S24 inv_S24_exceptTX2 (as claimed), inv_S24_parentWakes (unrestricted)   S25 inv_S25_cancelAtomic
 *   G2  inv_S24_parentWakes (T-X2 path)          G7  inv_S17_capAlways (RR_SNAPSHOT only)
 *   G10 inv_G10_noClaimAbort (one row aborts every topic's claims) G11 inv_G11_cancelReachesChildren
 *   N1  inv_N1_tx2OwnLineage (T-X2 lineage query on a child key: supersession and ReturnLatest)
 *   G1  inv_G1_noSelfCancelFromOperator (an operator pause/cancel during a claim costs restart budget)
 *   N2  inv_S13_topicConcurrency (executing handlers per topic can exceed the cap after cancel)
 *   N3  inv_S19_sourceTerminal (public StartOptions.restarted_from_workflow_id accepts a live source)
 *
 * Abstractions (INVARIANTS §7.2): payloads, state JSON, history events,
 * errors, backoff and the reconcile retry delay (nondeterministic 0/1 tick; a jittered
 * RetryPolicy delay can be 0), timeout/lease durations (only whether the bounds are valid),
 * registry filters (every runtime has every definition, F3), continuation
 * streaks, operator restart/retry, timers, approvals, schedules. Candidate order in
 * claims is a parameter, a superset of the code's (availableAt, id) order.
 */
module durable {
  //--------------------------------------------------------------- constants
  const RUNTIMES: Set[int]
  const MAX_WF: int
  const MAX_ACT: int
  const TOPICS: Set[str]
  const TOPIC_CAP: str -> int
  const LOCAL_SLOTS: int          // executor slots per runtime (sum of local topic caps)
  const MAX_ATTEMPTS: int         // activity maxAttempts at insert
  const MAX_ACTIVATION: int       // min(maxActivationAttempts, config)
  const LEASE_W: int              // workflow lease duration
  const LEASE_A: int              // activity lease duration
  const MAX_TIME: int             // clock bound
  const DRIFT: int                // local clock lag vs DB time (0 = S13 assumption holds)
  const RR_SNAPSHOT: bool         // HISTORICAL: T-W1 plain reads use an InnoDB REPEATABLE READ snapshot (pre-P4)
  const ENABLE_CHILDREN: bool
  const ENABLE_CRASH: bool
  const ENABLE_ENV_EDITS: bool    // external writes to durable rows (EnvCorruptActivityBounds)
  const MAX_TASK_RESTARTS: int    // RuntimeConfig.max_task_restarts (per task; supervisor.rs)

  // version of the trace-checking interface (action names and parameters, views)
  pure val TRACE_IFACE_VERSION = 4

  pure val WF_IDS = 1.to(MAX_WF)
  pure val ACT_IDS = 1.to(MAX_ACT)
  pure val ATT_NOS = 1.to(MAX_ATTEMPTS)
  pure val NO_KEY = 0
  pure val DOMAIN_KEY = 1         // the caller-supplied key "k"
  pure def autoKey(parent: int, cmd: int): int = 1000 + parent * 10 + cmd  // child:{parent}:{cmd}
  pure val CONTINUATION_READY_AT = 0  // transition.rs CONTINUATION_READY_AT_MILLIS

  //------------------------------------------------------------------- types
  type WfRow = {
    status: str, kind: str, dedup: int, waitKind: str, waitRef: int,
    availableAt: int, token: int, leaseExp: int, cmdSeq: int, delivered: int,
    actAttempts: int, parent: int, root: int, restartedFrom: int
  }
  type ActRow = {
    status: str, wf: int, topic: str, attemptCount: int, maxAttempts: int,
    availableAt: int, token: int, leaseExp: int,
    invalidBounds: bool           // timeout_millis <= 0 or lease_duration_millis <= timeout_millis
  }
  type AttRow = { used: bool, token: int, open: bool }
  type Event = { dseq: int, typ: str, ref: int, cmd: int }  // cmd = workflow commandSequence at append
  type Db = {
    wf: int -> WfRow, act: int -> ActRow, att: int -> (int -> AttRow),
    events: int -> Set[Event], nextToken: int, nextWf: int, nextAct: int
  }
  type Claim = { active: bool, wf: int, token: int, delivered: int }
  type Exec = { rt: int, act: int, attempt: int, token: int, maxAttempts: int, deadline: int, phase: str }
  type Hb = { id: int, rt: int, act: int, attempt: int, token: int, sentAt: int }
  // In-flight T-W1. phase: idle | topics (between topics) | reconcile | claim.
  // wAct/wWf/wAtt hold its own (uncommitted) writes for the rows in tA/tW.
  // lockA/lockW: rows it holds FOR UPDATE until COMMIT.
  type Tw1 = {
    phase: str, mode: str, now: int, topic: str, todo: Set[str], cands: Set[int], wanted: int,
    lockA: Set[int], lockW: Set[int], tA: Set[int], tW: Set[int],
    wAct: int -> ActRow, wWf: int -> WfRow, wAtt: int -> (int -> AttRow),
    claimed: Set[Exec], snap: bool, snapAct: int -> ActRow, snapWf: int -> WfRow
  }
  type Proc = { claims: int -> Claim, execs: Set[Exec], hbs: Set[Hb], tw1: int -> Tw1, topicHolder: int }
  type Ghost = {
    staleWfCommit: bool, staleActCommit: bool, capExceededAtClaim: bool,
    tx2OutsideLineage: bool, claimAborted: bool, noEventError: bool,
    revivedExpiredLease: bool, nextHb: int,
    lastIssuedWf: int -> int, lastIssuedAct: int -> int, taskErrors: int -> int,
    // restart budget per task (supervisor.rs: cumulative per TaskSpec, in process memory)
    coordErrors: int -> int, dispErrors: int -> int,
    // G1: the lease token an operator action revoked per workflow; operator-caused task errors
    opRevoked: int -> int, opTaskErrors: int -> int
  }

  //--------------------------------------------------------------- variables
  var now: int                    // DB time (UTC_TIMESTAMP)
  var db: Db                      // committed durable rows
  var proc: Proc                  // process-local state (and uncommitted T-W1 writes), lost on Crash
  var ghost: Ghost                // history variables for invariants; nextHb = next heartbeat id
  var prevWf: int -> str          // statuses in the previous state (action properties)
  var prevAct: int -> str
  var lastAction: str

  //----------------------------------------------------------------- helpers
  pure def min2(a: int, b: int): int = if (a < b) a else b
  pure def max2(a: int, b: int): int = if (a > b) a else b
  pure def isTerminal(s: str): bool = Set("succeeded", "failed", "cancelled").contains(s)
  pure def isLive(s: str): bool = s != "none" and not(isTerminal(s))

  pure val emptyWf: WfRow = {
    status: "none", kind: "", dedup: NO_KEY, waitKind: "", waitRef: 0,
    availableAt: 0, token: 0, leaseExp: 0, cmdSeq: 0, delivered: 0,
    actAttempts: 0, parent: 0, root: 0, restartedFrom: 0
  }
  pure val emptyAct: ActRow = {
    status: "none", wf: 0, topic: "", attemptCount: 0, maxAttempts: 0,
    availableAt: 0, token: 0, leaseExp: 0, invalidBounds: false
  }
  pure val noAtt: AttRow = { used: false, token: 0, open: false }
  pure val noClaim: Claim = { active: false, wf: 0, token: 0, delivered: 0 }
  pure val noExec: Exec = { rt: 0, act: 0, attempt: 0, token: 0, maxAttempts: 0, deadline: 0, phase: "none" }
  pure val noHb: Hb = { id: 0, rt: 0, act: 0, attempt: 0, token: 0, sentAt: 0 }
  pure val idleTw1: Tw1 = {
    phase: "idle", mode: "", now: 0, topic: "", todo: Set(), cands: Set(), wanted: 0,
    lockA: Set(), lockW: Set(), tA: Set(), tW: Set(),
    wAct: ACT_IDS.mapBy(a => emptyAct), wWf: WF_IDS.mapBy(w => emptyWf),
    wAtt: ACT_IDS.mapBy(a => ATT_NOS.mapBy(n => noAtt)),
    claimed: Set(), snap: false, snapAct: ACT_IDS.mapBy(a => emptyAct), snapWf: WF_IDS.mapBy(w => emptyWf)
  }

  pure def setWf(d: Db, w: int, r: WfRow): Db = d.with("wf", d.wf.set(w, r))
  pure def setAct(d: Db, a: int, r: ActRow): Db = d.with("act", d.act.set(a, r))
  pure def setAtt(d: Db, a: int, n: int, r: AttRow): Db = d.with("att", d.att.set(a, d.att.get(a).set(n, r)))

  pure def attOpenWith(d: Db, a: int, n: int, tok: int): bool =
    n >= 1 and n <= MAX_ATTEMPTS and d.att.get(a).get(n).open and d.att.get(a).get(n).token == tok

  // close the open attempt of the current attemptCount (finishedAt IS NULL fence)
  pure def closeAttempt(d: Db, a: int): Db = {
    val n = d.act.get(a).attemptCount
    if (n >= 1 and d.att.get(a).get(n).open) setAtt(d, a, n, d.att.get(a).get(n).with("open", false)) else d
  }

  // deliverySequence = deliveredEventSequence + 1; unique key on (wf, deliverySequence)
  pure def canAppend(d: Db, w: int): bool =
    not(d.events.get(w).exists(e => e.dseq == d.wf.get(w).delivered + 1))
  pure def appendDeliverable(d: Db, w: int, typ: str, ref: int): Db =
    d.with("events", d.events.set(w, d.events.get(w).union(Set({ dseq: d.wf.get(w).delivered + 1, typ: typ, ref: ref, cmd: d.wf.get(w).cmdSeq }))))

  pure def waitsOnAct(d: Db, w: int, a: int): bool =
    d.wf.get(w).status == "waiting_activity" and d.wf.get(w).waitRef == a

  pure def rootOf(d: Db, w: int): int = if (d.wf.get(w).root != 0) d.wf.get(w).root else w

  // wake_waiting_parents_on_child_terminal (src/persistence/workflows.rs): a waiting_child parent,
  // or a paused one whose wait is on c, gets the outcome; a paused parent stays paused (wait cleared)
  pure def waitsOnChild(r: WfRow, c: int): bool =
    (r.status == "waiting_child" or r.status == "paused") and r.waitKind == "child" and r.waitRef == c
  pure def wakeParentsOk(d: Db, c: int): bool =
    WF_IDS.forall(p => waitsOnChild(d.wf.get(p), c) implies canAppend(d, p))
  pure def wakeParents(d: Db, c: int, typ: str, t: int): Db =
    WF_IDS.fold(d, (acc, p) => {
      val pr = acc.wf.get(p)
      if (waitsOnChild(pr, c))
        setWf(appendDeliverable(acc, p, typ, c), p,
              pr.with("status", if (pr.status == "paused") "paused" else "ready")
                .with("waitKind", "").with("waitRef", 0).with("availableAt", t))
      else acc
    })
  pure def childOutcome(s: str): str =
    if (s == "succeeded") "child_succeeded" else if (s == "failed") "child_failed" else "child_cancelled"

  // cancel_activities (src/store.rs)
  pure def cancelActivities(d: Db, w: int): Db =
    ACT_IDS.fold(d, (acc, a) => {
      val ar = acc.act.get(a)
      if (ar.wf == w and (ar.status == "pending" or ar.status == "running"))
        setAct(if (ar.status == "running") closeAttempt(acc, a) else acc, a,
               ar.with("status", "cancelled").with("token", 0).with("leaseExp", 0))
      else acc
    })

  // follow restartedFrom from x; true iff orig is on the chain
  pure def inLineage(d: Db, x: int, orig: int): bool = {
    val walk = WF_IDS.fold({ cur: x, found: x == orig }, (st, i) => {
      val nxt = if (st.cur == 0) 0 else d.wf.get(st.cur).restartedFrom
      { cur: nxt, found: st.found or (nxt != 0 and nxt == orig) }
    })
    walk.found
  }

  pure def maxOf(s: Set[int]): int = s.fold(0, (m, x) => if (x > m) x else m)

  //------------------------------------------------------ T-W1 row locks
  pure def actLocked(p: Proc, a: int): bool = RUNTIMES.exists(r => p.tw1.get(r).lockA.contains(a))
  pure def wfLocked(p: Proc, w: int): bool = RUNTIMES.exists(r => p.tw1.get(r).lockW.contains(w))

  //------------------------------------------------------ T-W1 read views
  // current read (locking read / UPDATE): own writes, else latest committed
  pure def overlay(d: Db, tw: Tw1): Db =
    d.with("act", ACT_IDS.mapBy(a => if (tw.tA.contains(a)) tw.wAct.get(a) else d.act.get(a)))
     .with("att", ACT_IDS.mapBy(a => if (tw.tA.contains(a)) tw.wAtt.get(a) else d.att.get(a)))
     .with("wf", WF_IDS.mapBy(w => if (tw.tW.contains(w)) tw.wWf.get(w) else d.wf.get(w)))
  // plain (consistent) read: READ COMMITTED = latest committed at statement start
  // (plus own writes); historical REPEATABLE READ = the snapshot (plus own writes)
  pure def plainAct(d: Db, tw: Tw1, a: int): ActRow =
    if (tw.tA.contains(a)) tw.wAct.get(a) else if (RR_SNAPSHOT) tw.snapAct.get(a) else d.act.get(a)
  pure def plainWf(d: Db, tw: Tw1, w: int): WfRow =
    if (tw.tW.contains(w)) tw.wWf.get(w) else if (RR_SNAPSHOT) tw.snapWf.get(w) else d.wf.get(w)
  // store a view back as own writes
  pure def keep(tw: Tw1, v: Db): Tw1 = tw.with("wAct", v.act).with("wWf", v.wf).with("wAtt", v.att)

  pure def hereOn(tw: Tw1, t: str): int = tw.claimed.filter(e => e.act != 0 and tw.wAct.get(e.act).topic == t).size()
  pure def topicDone(tw: Tw1): bool =
    tw.phase == "topics" or (tw.phase == "claim" and (tw.cands == Set() or hereOn(tw, tw.topic) >= tw.wanted))

  pure def liveOnTopic(d: Db, t: str, tnow: int): int =
    ACT_IDS.filter(a => d.act.get(a).topic == t and d.act.get(a).status == "running" and d.act.get(a).leaseExp > tnow).size()

  //----------------------------------------------------------------- commit
  action commit(d: Db, p: Proc, g: Ghost, name: str): bool = all {
    db' = d, proc' = p, ghost' = g, now' = now, lastAction' = name,
  }
  // a DB transaction that observed DB time tnow
  action commitAt(d: Db, p: Proc, g: Ghost, name: str, tnow: int): bool = all {
    db' = d, proc' = p, ghost' = g, now' = max2(now, tnow), lastAction' = name,
  }

  //------------------------------------------------ views for trace comparison
  // WfRow {status, kind, dedup, waitKind, waitRef, availableAt, token, leaseExp, cmdSeq,
  //        delivered, actAttempts, parent, root, restartedFrom}
  pure def viewWf(d: Db, w: int): WfRow = d.wf.get(w)
  // ActRow {status, wf, topic, attemptCount, maxAttempts, availableAt, token, leaseExp, invalidBounds}
  pure def viewAct(d: Db, a: int): ActRow = d.act.get(a)
  // AttRow {used, token, open}
  pure def viewAtt(d: Db, a: int, n: int): AttRow = d.att.get(a).get(n)
  // deliverable events {dseq, typ, ref, cmd}
  pure def deliverable(d: Db, w: int): Set[Event] = d.events.get(w)

  //---------------------------------------------------------- application
  pure def newStarted(d: Db, w: int, r: WfRow): Db = {
    val d1 = setWf(d, w, r)
    d1.with("events", d1.events.set(w, Set({ dseq: 1, typ: "started", ref: 0, cmd: 0 }))).with("nextWf", w + 1)
  }
  pure def rowsWithKey(d: Db, kind: str, key: int): Set[int] =
    WF_IDS.filter(x => key != NO_KEY and d.wf.get(x).status != "none" and d.wf.get(x).kind == kind and d.wf.get(x).dedup == key)
  pure def successorsOf(d: Db, src: int): Set[int] =
    WF_IDS.filter(x => src != 0 and d.wf.get(x).status != "none" and d.wf.get(x).restartedFrom == src)
  pure def anyOf(s: Set[int]): int = s.fold(0, (acc, x) => x)

  // T-X1: start_with_conn -> insert_prepared -> insert_started. `from` =
  // StartOptions.restarted_from_workflow_id (0 = none). Both a key and `from` ->
  // InvalidDefinition before any SQL (not a step).
  //   inserted = true : wNew = db.nextWf, a new ready row
  //   inserted = false, key != 0 : dedup hit, wNew = the existing row, no write
  //   inserted = false, key == 0 : restart-key collision -> Conflict, wNew = 0, no row
  action TX1_Start(wNew: int, kind: str, key: int, from: int, inserted: bool, tnow: int): bool = {
    val existing = rowsWithKey(db, kind, key)
    val succ = successorsOf(db, from)
    all {
      not(key != NO_KEY and from != 0),
      from == 0 or (WF_IDS.contains(from) and db.wf.get(from).status != "none"),
      if (inserted) all {
        existing == Set(), succ == Set(), wNew == db.nextWf, wNew <= MAX_WF,
        commitAt(newStarted(db, wNew, { ...emptyWf, status: "ready", kind: kind, dedup: key, availableAt: tnow, restartedFrom: from }),
                 proc, ghost, "TX1_Start", tnow),
      } else if (key != NO_KEY) all {
        existing.contains(wNew), commitAt(db, proc, ghost, "TX1_StartExisting", tnow),
      } else all {
        wNew == 0, succ != Set(), succ.forall(x => not(wfLocked(proc, x))), commitAt(db, proc, ghost, "TX1_StartConflict", tnow),
      },
    }
  }

  // T-X2 lookups: the original (kind, key) row and the newest row of its "lineage":
  // same kind and (id = root or rootWorkflowId = root), root = original's root
  pure def tx2Orig(d: Db, kind: str, key: int): int = anyOf(rowsWithKey(d, kind, key))
  pure def tx2Latest(d: Db, kind: str, key: int): int = {
    val orig = tx2Orig(d, kind, key)
    val root = if (orig == 0) 0 else rootOf(d, orig)
    maxOf(WF_IDS.filter(x => orig != 0 and d.wf.get(x).status != "none" and d.wf.get(x).kind == kind
                             and (x == root or d.wf.get(x).root == root)))
  }

  // T-X2: start_or_restart_recoverable_with_conn(kind, key). orig/latest are the rows the
  // code locks (0 = none); sNew is the inserted row (0 = none).
  //   orig = 0                          : insert (kind, key)           -> TX2_StartNew
  //   superseded = false, latest live   : return latest, no write      -> TX2_ReturnLatest
  //   superseded = false, successor exists: Conflict, all rolled back  -> TX2_Conflict
  //   superseded = true                 : cancel latest's dead letters, blocked -> cancelled, successor sNew
  action TX2_RecoverableStart(kind: str, key: int, orig: int, latest: int, superseded: bool, sNew: int, tnow: int): bool = {
    val root = if (orig == 0) 0 else rootOf(db, orig)
    val lr = db.wf.get(latest)
    val failedOrBlocked = latest != 0 and (lr.status == "failed" or lr.status == "blocked")
    val succ = successorsOf(db, latest)
    all {
      key != NO_KEY,
      orig == tx2Orig(db, kind, key), latest == tx2Latest(db, kind, key),
      orig == 0 or not(wfLocked(proc, orig)),
      latest == 0 or not(wfLocked(proc, latest)),
      if (orig == 0) all {
        not(superseded), sNew == db.nextWf, sNew <= MAX_WF,
        commitAt(newStarted(db, sNew, { ...emptyWf, status: "ready", kind: kind, dedup: key, availableAt: tnow }),
                 proc, ghost, "TX2_StartNew", tnow),
      } else if (not(superseded)) all {
        sNew == 0,
        // N1: the returned row need not be on orig's restart chain (an unrelated sibling)
        if (not(failedOrBlocked))
          commitAt(db, proc, ghost.with("tx2OutsideLineage", ghost.tx2OutsideLineage or not(inLineage(db, latest, orig))),
                   "TX2_ReturnLatest", tnow)
        else all { succ != Set(), succ.forall(x => not(wfLocked(proc, x))), commitAt(db, proc, ghost, "TX2_Conflict", tnow) },
      } else all {
        failedOrBlocked, succ == Set(), sNew == db.nextWf, sNew <= MAX_WF,
        not(ACT_IDS.exists(a => db.act.get(a).wf == latest and actLocked(proc, a))),
        // cancel its dead_lettered activities
        val d1 = ACT_IDS.fold(db, (acc, a) =>
          if (acc.act.get(a).wf == latest and acc.act.get(a).status == "dead_lettered")
            setAct(acc, a, acc.act.get(a).with("status", "cancelled")) else acc)
        // blocked -> cancelled, wait fields KEPT, no parent wake (G2)
        val d2 = if (lr.status == "blocked") setWf(d1, latest, lr.with("status", "cancelled").with("token", 0).with("leaseExp", 0)) else d1
        val d3 = newStarted(d2, sNew, { ...emptyWf, status: "ready", kind: kind, availableAt: tnow, root: root, restartedFrom: latest })
        commitAt(d3, proc, ghost.with("tx2OutsideLineage", ghost.tx2OutsideLineage or not(inLineage(db, latest, orig))),
                 "TX2_RecoverableStart", tnow),
      },
    }
  }

  // store.rs cancel_locked_workflow (T-X3 and T-A4): cancel_activities, fenced workflow ->
  // cancelled (wait and lease cleared), wake waiting parents with child_cancelled
  def cancelOk(w: int): bool = all {
    WF_IDS.contains(w),
    isLive(db.wf.get(w).status),
    not(wfLocked(proc, w)),
    not(ACT_IDS.exists(a => db.act.get(a).wf == w and actLocked(proc, a))),
  }
  pure def cancelWrite(d: Db, w: int): Db =
    setWf(cancelActivities(d, w), w,
          d.wf.get(w).with("status", "cancelled").with("waitKind", "").with("waitRef", 0).with("token", 0).with("leaseExp", 0))

  // T-X3: cancel_with_conn; on a terminal row it is a no-op (not a step)
  action TX3_Cancel(w: int, tnow: int): bool = all {
    cancelOk(w),
    wakeParentsOk(cancelWrite(db, w), w),
    commitAt(wakeParents(cancelWrite(db, w), w, "child_cancelled", tnow), proc, ghost, "TX3_Cancel", tnow),
  }

  //------------------------------------------------------------ coordinator
  // T-C1: at most one expired-lease recovery (rec), then at most one ready claim (cl) with
  // lease token tok = db.nextToken and lease expiry leaseExp, one txn (SKIP LOCKED).
  // rec / cl = 0 when absent (then tok and leaseExp are ignored).
  action TC1_Claim(r: int, rec: int, cl: int, tok: int, leaseExp: int, tnow: int): bool = {
    val doRec = rec != 0
    val doCl = cl != 0
    val rr = db.wf.get(rec)
    val d1 = if (doRec) setWf(db, rec, rr.with("status", "ready").with("availableAt", tnow).with("token", 0).with("leaseExp", 0)) else db
    val cr = d1.wf.get(cl)
    all {
      RUNTIMES.contains(r),
      not(stoppedRt(ghost, r)),
      not(proc.claims.get(r).active),
      doRec or doCl,
      doRec implies (WF_IDS.contains(rec) and rr.status == "running" and rr.leaseExp <= tnow and not(wfLocked(proc, rec))),
      doCl implies (WF_IDS.contains(cl) and cr.status == "ready" and cr.availableAt <= tnow and not(wfLocked(proc, cl))
                    and tok == db.nextToken and leaseExp > tnow),
      if (doCl)
        commitAt(setWf(d1, cl, cr.with("status", "running").with("token", tok).with("leaseExp", leaseExp)).with("nextToken", tok + 1),
                 proc.with("claims", proc.claims.set(r, { active: true, wf: cl, token: tok, delivered: cr.delivered })),
                 ghost.with("lastIssuedWf", ghost.lastIssuedWf.set(cl, tok)), "TC1_Claim", tnow)
      else commitAt(d1, proc, ghost, "TC1_Recover", tnow),
    }
  }

  // the supervisor cancels the whole runtime once a task's restart count exceeds MAX_TASK_RESTARTS
  pure def stoppedRt(g: Ghost, r: int): bool =
    g.coordErrors.get(r) > MAX_TASK_RESTARTS or g.dispErrors.get(r) > MAX_TASK_RESTARTS
  pure def coordError(g: Ghost, r: int): Ghost =
    g.with("taskErrors", g.taskErrors.setBy(r, x => x + 1)).with("coordErrors", g.coordErrors.setBy(r, x => x + 1))

  pure def wfFenceOk(d: Db, c: Claim): bool =
    c.active and d.wf.get(c.wf).status == "running" and d.wf.get(c.wf).token == c.token
  def holds(r: int, w: int, tok: int): bool =
    RUNTIMES.contains(r) and proc.claims.get(r).active and proc.claims.get(r).wf == w and proc.claims.get(r).token == tok

  // L-C1 found no deliverable event after `delivered` -> InvalidState -> task error (S5d defence)
  action LC1_NoEvent(r: int, w: int): bool = {
    val c = proc.claims.get(r)
    all {
      RUNTIMES.contains(r), c.active, c.wf == w,
      not(db.events.get(w).exists(e => e.dseq == c.delivered + 1)),
      commit(db, proc.with("claims", proc.claims.set(r, noClaim)),
             coordError(ghost, r).with("noEventError", true), "LC1_NoEvent"),
    }
  }

  // T-C2 / T-C3 lose the fence: rollback, FencedWrite propagates, coordinator task restarts (G1).
  // When an operator pause/cancel revoked this token, the error is operator-caused.
  action CoordFenceMiss(r: int, w: int, tok: int): bool = {
    val op = tok != 0 and WF_IDS.contains(w) and ghost.opRevoked.get(w) == tok
    all {
      holds(r, w, tok),
      not(wfFenceOk(db, proc.claims.get(r))),
      commit(db, proc.with("claims", proc.claims.set(r, noClaim)),
             coordError(ghost, r).with("opTaskErrors", ghost.opTaskErrors.setBy(r, x => if (op) x + 1 else x)),
             "CoordFenceMiss"),
    }
  }

  // T-C2 common part: claim held, fence holds, the consumed event exists
  def tc2Ok(r: int, w: int, tok: int): bool = all {
    holds(r, w, tok),
    wfFenceOk(db, proc.claims.get(r)),
    db.events.get(w).exists(e => e.dseq == proc.claims.get(r).delivered + 1),
  }
  def tc2Base(r: int, w: int): WfRow =
    db.wf.get(w).with("delivered", proc.claims.get(r).delivered + 1).with("token", 0).with("leaseExp", 0).with("actAttempts", 0)
  def tc2Proc(r: int): Proc = proc.with("claims", proc.claims.set(r, noClaim))
  def tc2Ghost(w: int, tok: int): Ghost = ghost.with("staleWfCommit", ghost.staleWfCommit or tok != ghost.lastIssuedWf.get(w))

  // T-C2 Continue: ready again at availableAt (>= tnow), `continued` event appended
  action TC2_Continue(r: int, w: int, tok: int, availableAt: int, tnow: int): bool = {
    val d1 = setWf(db, w, tc2Base(r, w).with("status", "ready").with("availableAt", availableAt))
    all { tc2Ok(r, w, tok), availableAt >= tnow, canAppend(d1, w),
          commitAt(appendDeliverable(d1, w, "continued", 0), tc2Proc(r), tc2Ghost(w, tok), "TC2_Continue", tnow) }
  }

  // T-C2 Complete: succeeded, waiting parents woken in the same txn
  action TC2_Complete(r: int, w: int, tok: int, tnow: int): bool = {
    val d1 = setWf(db, w, tc2Base(r, w).with("status", "succeeded"))
    all { tc2Ok(r, w, tok), wakeParentsOk(d1, w),
          commitAt(wakeParents(d1, w, "child_succeeded", tnow), tc2Proc(r), tc2Ghost(w, tok), "TC2_Complete", tnow) }
  }

  // T-C2 RunActivity: new activity aNew = db.nextAct on `topic`, waiting_activity.
  // invalidBounds: the command's timeout/lease bounds are invalid (ActivityCommand::new rejects
  // that; a serde-built command does not), only when ENABLE_ENV_EDITS.
  // prio: continuation priority (coordinator.rs commit_activity) -> availableAt is
  // CONTINUATION_READY_AT, before any DB time; else availableAt = now (>= tnow)
  action TC2_RunActivity(r: int, w: int, tok: int, aNew: int, topic: str, maxAttempts: int, invalidBounds: bool,
                         prio: bool, availableAt: int, tnow: int): bool = {
    val d1 = setAct(db, aNew, { status: "pending", wf: w, topic: topic, attemptCount: 0, maxAttempts: maxAttempts,
                                availableAt: availableAt, token: 0, leaseExp: 0, invalidBounds: invalidBounds })
               .with("nextAct", aNew + 1)
    val cmd = db.wf.get(w).cmdSeq + 1
    all {
      tc2Ok(r, w, tok), aNew == db.nextAct, aNew <= MAX_ACT, TOPICS.contains(topic),
      1 <= maxAttempts, maxAttempts <= MAX_ATTEMPTS, invalidBounds implies ENABLE_ENV_EDITS,
      if (prio) availableAt == CONTINUATION_READY_AT else availableAt >= tnow,
      commitAt(setWf(d1, w, tc2Base(r, w).with("status", "waiting_activity").with("waitKind", "activity").with("waitRef", aNew).with("cmdSeq", cmd)),
               tc2Proc(r), tc2Ghost(w, tok), "TC2_RunActivity", tnow),
    }
  }

  // T-C2 RunChild(kind, key): key is a domain key or autoKey(w, cmd). existing = the row
  // with (kind, key), 0 = none: then insert cNew = db.nextWf; else attach (cNew = 0),
  // locking the child and waking at once if it is already terminal.
  action TC2_RunChild(r: int, w: int, tok: int, kind: str, key: int, existing: int, cNew: int, tnow: int): bool = {
    val cmd = db.wf.get(w).cmdSeq + 1
    val waitOn = if (existing == 0) cNew else existing
    val pw = tc2Base(r, w).with("status", "waiting_child").with("waitKind", "child").with("waitRef", waitOn).with("cmdSeq", cmd)
    all {
      ENABLE_CHILDREN,
      tc2Ok(r, w, tok), key != NO_KEY,
      existing == anyOf(rowsWithKey(db, kind, key)),
      if (existing == 0) all {
        cNew == db.nextWf, cNew <= MAX_WF,
        val d2 = newStarted(db, cNew, { ...emptyWf, status: "ready", kind: kind, dedup: key, availableAt: tnow,
                                        parent: w, root: rootOf(db, w) })
        commitAt(setWf(d2, w, pw), tc2Proc(r), tc2Ghost(w, tok), "TC2_RunChild_New", tnow),
      } else {
        val d1 = setWf(db, w, pw)
        val cs = d1.wf.get(existing).status
        all {
          cNew == 0,
          not(wfLocked(proc, existing)),
          if (isTerminal(cs))
            all { wakeParentsOk(d1, existing),
                  commitAt(wakeParents(d1, existing, childOutcome(cs), tnow), tc2Proc(r), tc2Ghost(w, tok), "TC2_RunChild_AttachTerminal", tnow) }
          else commitAt(d1, tc2Proc(r), tc2Ghost(w, tok), "TC2_RunChild_Attach", tnow),
        }
      },
    }
  }

  // T-C3: activation failure (handler error); event not consumed. attempt = activationAttempts + 1;
  // attempt >= maxActivation -> failed (+ parent wake), else ready. Both branches write
  // availableAt = now + backoff (coordinator.rs record_activation_failure), so it is >= tnow.
  action TC3_ActivationFailure(r: int, w: int, tok: int, attempt: int, maxActivation: int, availableAt: int, tnow: int): bool = {
    val row = db.wf.get(w)
    val p1 = proc.with("claims", proc.claims.set(r, noClaim))
    val g1 = tc2Ghost(w, tok)
    all {
      holds(r, w, tok),
      wfFenceOk(db, proc.claims.get(r)),
      attempt == row.actAttempts + 1,
      availableAt >= tnow,
      if (attempt >= maxActivation) {
        val d1 = setWf(db, w, row.with("status", "failed").with("actAttempts", attempt).with("availableAt", availableAt)
                                .with("token", 0).with("leaseExp", 0))
        all { wakeParentsOk(d1, w), commitAt(wakeParents(d1, w, "child_failed", tnow), p1, g1, "TC3_Exhausted", tnow) }
      } else all {
        commitAt(setWf(db, w, row.with("status", "ready").with("actAttempts", attempt).with("availableAt", availableAt)
                                .with("token", 0).with("leaseExp", 0)), p1, g1, "TC3_Retry", tnow),
      },
    }
  }

  //------------------------------------------------------ activity dispatcher (T-W1)
  pure def localAvail(p: Proc, r: int): int = LOCAL_SLOTS - p.execs.filter(e => e.rt == r).size()

  action setTw1(r: int, tw: Tw1, name: str): bool = commit(db, proc.with("tw1", proc.tw1.set(r, tw)), ghost, name)

  pure def beginTw1(d: Db, mode: str, tnow: int, todo: Set[str]): Tw1 =
    { ...idleTw1, phase: "topics", mode: mode, now: tnow, todo: todo, wAct: d.act, wWf: d.wf, wAtt: d.att }

  // reconcile of one expired running activity (lease_expired): exhausted -> dead_lettered at
  // availableAt = tnow and the waiting workflow blocked; else pending at availableAt = tnow +
  // the retry policy's delay (activity_worker.rs reconcile_expired); attempt closed
  pure def reconcileExhausted(v: Db, a: int): bool = v.act.get(a).attemptCount >= v.act.get(a).maxAttempts
  pure def reconcileAvailOk(v: Db, a: int, tnow: int, availableAt: int): bool =
    if (reconcileExhausted(v, a)) availableAt == tnow else availableAt >= tnow
  pure def reconcileWrite(v: Db, a: int, availableAt: int): Db = {
    val cur = v.act.get(a)
    val exhausted = reconcileExhausted(v, a)
    val w = cur.wf
    val wr = v.wf.get(w)
    val d2 = setAct(closeAttempt(v, a), a, cur.with("status", if (exhausted) "dead_lettered" else "pending")
                                             .with("availableAt", availableAt).with("token", 0).with("leaseExp", 0))
    if (exhausted and wr.status == "waiting_activity" and wr.waitRef == a) setWf(d2, w, wr.with("status", "blocked")) else d2
  }
  pure def claimWrite(v: Db, a: int, tok: int, leaseExp: int): Db = {
    val cur = v.act.get(a)
    val n = cur.attemptCount + 1
    val d1 = setAct(v, a, cur.with("status", "running").with("attemptCount", n).with("token", tok).with("leaseExp", leaseExp))
    setAtt(d1, a, n, { used: true, token: tok, open: true }).with("nextToken", tok + 1)
  }
  pure def claimable(v: Db, a: int, tnow: int): bool = {
    val cur = v.act.get(a)
    cur.status == "pending" and cur.availableAt <= tnow and waitsOnAct(v, cur.wf, a)
  }

  // --- simulation form: one action per statement group (READ COMMITTED interleavings) ---

  // claim_batch: lock every registered topic row FOR UPDATE SKIP LOCKED; fewer than all -> empty
  // result (modelled as disabled); then sample DB now. `limit`/local capacity must be positive.
  action TW1_BeginBatch(r: int): bool = all {
    not(stoppedRt(ghost, r)),
    proc.topicHolder == 0,
    proc.tw1.get(r).phase == "idle",
    localAvail(proc, r) > 0,
    commit(db, proc.with("topicHolder", r).with("tw1", proc.tw1.set(r, beginTw1(db, "batch", now, TOPICS))), ghost, "TW1_BeginBatch"),
  }

  // claim_one(t): DB now sampled first (tnow), then the topic row FOR UPDATE (blocking, no
  // SKIP LOCKED), so tnow may lag `now` by the lock wait (any tnow <= now is allowed).
  action TW1_BeginOne(r: int, t: str, tnow: int): bool = all {
    not(stoppedRt(ghost, r)),
    proc.topicHolder == 0,
    proc.tw1.get(r).phase == "idle",
    TOPICS.contains(t),
    0 <= tnow, tnow <= now,
    commit(db, proc.with("topicHolder", r).with("tw1", proc.tw1.set(r, beginTw1(db, "one", tnow, Set(t)))), ghost, "TW1_BeginOne"),
  }

  // reconcile_expired, statement 1: plain read of expired running rows on topic t
  // (the first plain read of T-W1: under historical RR it fixes the snapshot)
  action TW1_ReconcileScan(r: int, t: str): bool = {
    val tw0 = proc.tw1.get(r)
    val tw = if (RR_SNAPSHOT and not(tw0.snap)) tw0.with("snap", true).with("snapAct", db.act).with("snapWf", db.wf) else tw0
    val cands = ACT_IDS.filter(a => {
      val v = plainAct(db, tw, a)
      v.topic == t and v.status == "running" and v.leaseExp <= tw.now
    })
    all {
      tw0.phase != "idle", topicDone(tw0), tw0.todo.contains(t),
      setTw1(r, tw.with("phase", "reconcile").with("topic", t).with("todo", tw0.todo.exclude(Set(t))).with("cands", cands),
             "TW1_ReconcileScan"),
    }
  }

  // reconcile_expired, per candidate: workflow FOR UPDATE, activity FOR UPDATE with the
  // filters rechecked (current read), then the updates (buffered until COMMIT).
  // availableAt: the reconciled row's new available_at (ignored when the recheck skips it)
  action TW1_ReconcileRow(r: int, a: int, availableAt: int): bool = {
    val tw = proc.tw1.get(r)
    val v = overlay(db, tw)
    val cur = v.act.get(a)
    val w = cur.wf
    val hit = cur.status == "running" and cur.topic == tw.topic and cur.leaseExp <= tw.now
    val block = cur.attemptCount >= cur.maxAttempts and waitsOnAct(v, w, a)
    val tw1 = tw.with("cands", tw.cands.exclude(Set(a))).with("lockW", tw.lockW.union(Set(w)))
    all {
      tw.phase == "reconcile", tw.cands.contains(a),
      if (hit) and {
        reconcileAvailOk(v, a, tw.now, availableAt),
        setTw1(r, keep(tw1, reconcileWrite(v, a, availableAt)).with("tA", tw.tA.union(Set(a))).with("lockA", tw.lockA.union(Set(a)))
                   .with("tW", if (block) tw.tW.union(Set(w)) else tw.tW), "TW1_ReconcileRow"),
      }
      else setTw1(r, tw1, "TW1_ReconcileSkip"),
    }
  }

  // in_flight count, then (if wanted > 0) the candidate join; both plain reads
  action TW1_Count(r: int): bool = {
    val tw = proc.tw1.get(r)
    val t = tw.topic
    val inFlight = ACT_IDS.filter(a => {
      val v = plainAct(db, tw, a)
      v.topic == t and v.status == "running" and v.leaseExp > tw.now
    }).size()
    val limit = if (tw.mode == "one") 1 else localAvail(proc, r)
    val wanted = min2(TOPIC_CAP.get(t) - inFlight, limit - tw.claimed.size())
    val cands = ACT_IDS.filter(a => {
      val v = plainAct(db, tw, a)
      v.topic == t and v.status == "pending" and v.availableAt <= tw.now
        and plainWf(db, tw, v.wf).status == "waiting_activity" and plainWf(db, tw, v.wf).waitRef == a
    })
    all {
      tw.phase == "reconcile", tw.cands == Set(),
      if (wanted > 0) setTw1(r, tw.with("phase", "claim").with("wanted", wanted).with("cands", cands), "TW1_Count")
      else setTw1(r, tw.with("phase", "topics").with("wanted", wanted).with("cands", Set()), "TW1_CountFull"),
    }
  }

  // claim_locked_candidate: workflow and activity FOR UPDATE SKIP LOCKED with the filters
  // rechecked (current read); lease token tok = db.nextToken (a UUID: consumed even if T-W1
  // later rolls back). Attempt-cap overrun or invalid bounds is TW1_Error instead.
  action TW1_ClaimRow(r: int, a: int, tok: int): bool = {
    val tw = proc.tw1.get(r)
    val v = overlay(db, tw)
    val cur = v.act.get(a)
    val w = cur.wf
    val e = { rt: r, act: a, attempt: cur.attemptCount + 1, token: tok, maxAttempts: cur.maxAttempts,
              deadline: tw.now + LEASE_A - 1 + DRIFT, phase: "running" }
    val d2 = claimWrite(v, a, tok, tw.now + LEASE_A)
    all {
      tw.phase == "claim", tw.cands.contains(a), hereOn(tw, tw.topic) < tw.wanted,
      claimable(v, a, tw.now),
      cur.attemptCount + 1 <= cur.maxAttempts,
      not(cur.invalidBounds),
      tok == db.nextToken,
      commit(db.with("nextToken", tok + 1),
             proc.with("tw1", proc.tw1.set(r, keep(tw, d2).with("tA", tw.tA.union(Set(a)))
                                                .with("lockA", tw.lockA.union(Set(a))).with("lockW", tw.lockW.union(Set(w)))
                                                .with("cands", tw.cands.exclude(Set(a)))
                                                .with("claimed", tw.claimed.union(Set(e))))),
             ghost, "TW1_ClaimRow"),
    }
  }

  // claim_locked_candidate returns None: recheck failed, or SKIP LOCKED passed a row that
  // another in-flight transaction holds
  action TW1_SkipRow(r: int, a: int): bool = {
    val tw = proc.tw1.get(r)
    all {
      tw.phase == "claim", tw.cands.contains(a), hereOn(tw, tw.topic) < tw.wanted,
      setTw1(r, tw.with("cands", tw.cands.exclude(Set(a))), "TW1_SkipRow"),
    }
  }

  pure def tw1Ghost(g: Ghost, d: Db, claimed: Set[Exec], tnow: int): Ghost =
    g.with("capExceededAtClaim", g.capExceededAtClaim or TOPICS.exists(t => liveOnTopic(d, t, tnow) > TOPIC_CAP.get(t)))
     .with("lastIssuedAct", claimed.fold(g.lastIssuedAct, (m, e) => m.set(e.act, e.token)))

  // COMMIT: own writes become visible, executions start, locks released
  action TW1_Commit(r: int): bool = {
    val tw = proc.tw1.get(r)
    val d = overlay(db, tw)
    all {
      tw.phase != "idle", topicDone(tw), tw.todo == Set(),
      commit(d, proc.with("topicHolder", 0).with("tw1", proc.tw1.set(r, idleTw1)).with("execs", proc.execs.union(tw.claimed)),
             tw1Ghost(ghost, d, tw.claimed, tw.now), "TW1_Commit"),
    }
  }

  // --- replay form: one committed T-W1 in one step ---
  // reconciled: expired rows the transaction reconciled (in order); claimed: rows it claimed
  // (in order) with their tokens and lease expiries; inFlightSeen / localAvail: per-topic
  // values the transaction observed (maps over all of TOPICS). Guards check only the
  // current-read fences; inv_S17_capAtClaim judges the claim count against live leases.
  pure def replayReconcile(d0: Db, rec: List[{ a: int, exhausted: bool, availableAt: int }], tnow: int): { d: Db, ok: bool } =
    rec.foldl({ d: d0, ok: true }, (st, x) =>
      if (not(ACT_IDS.contains(x.a))) st.with("ok", false)
      else {
        val cur = st.d.act.get(x.a)
        if (cur.status == "running" and cur.leaseExp <= tnow and x.exhausted == reconcileExhausted(st.d, x.a)
            and reconcileAvailOk(st.d, x.a, tnow, x.availableAt))
          st.with("d", reconcileWrite(st.d, x.a, x.availableAt))
        else st.with("ok", false)
      })
  pure def replayClaims(d0: Db, cl: List[{ a: int, tok: int, leaseExp: int }], r: int, tnow: int): { d: Db, ok: bool, execs: Set[Exec] } =
    cl.foldl({ d: d0, ok: true, execs: Set() }, (st, x) =>
      if (not(ACT_IDS.contains(x.a))) st.with("ok", false)
      else {
        val cur = st.d.act.get(x.a)
        if (claimable(st.d, x.a, tnow) and cur.attemptCount + 1 <= cur.maxAttempts and not(cur.invalidBounds) and x.tok == st.d.nextToken and x.leaseExp > tnow)
          { d: claimWrite(st.d, x.a, x.tok, x.leaseExp), ok: st.ok,
            execs: st.execs.union(Set({ rt: r, act: x.a, attempt: cur.attemptCount + 1, token: x.tok, maxAttempts: cur.maxAttempts,
                                        deadline: x.leaseExp - 1 + DRIFT, phase: "running" })) }
        else st.with("ok", false)
      })

  action TW1_Claim(r: int, tnow: int, localAvailSeen: str -> int, reconciled: List[{ a: int, exhausted: bool, availableAt: int }],
                   inFlightSeen: str -> int, claimed: List[{ a: int, tok: int, leaseExp: int }]): bool = {
    val rec = replayReconcile(db, reconciled, tnow)
    val cl = replayClaims(rec.d, claimed, r, tnow)
    all {
      RUNTIMES.contains(r), not(stoppedRt(ghost, r)),
      proc.topicHolder == 0, proc.tw1.get(r).phase == "idle",
      localAvailSeen.keys() == TOPICS, inFlightSeen.keys() == TOPICS,
      rec.ok, cl.ok,
      TOPICS.forall(t => {
        val n = cl.execs.filter(e => cl.d.act.get(e.act).topic == t).size()
        n <= TOPIC_CAP.get(t) - inFlightSeen.get(t) and n <= localAvailSeen.get(t)
      }),
      commitAt(cl.d, proc.with("execs", proc.execs.union(cl.execs)), tw1Ghost(ghost, cl.d, cl.execs, tnow), "TW1_Claim", tnow),
    }
  }

  // G10: claim_locked_candidate locks a claimable row a and finds it past its attempt cap
  // ("attempt_cap", checked first) or with invalid timeout/lease bounds ("invalid_bounds") ->
  // error: the whole T-W1 rolls back (every topic's reconciles and claims) and the dispatcher
  // task ends (task error). In simulation it aborts r's in-flight T-W1 at a candidate the claim
  // loop would reach; in replay (none in flight) it needs row a to be such a row at `now`.
  // ("missing_definition" is not modeled: every runtime has every definition, F3.)
  pure def claimErrorRow(v: Db, a: int, tnow: int, reason: str): bool = {
    val cur = v.act.get(a)
    val overCap = cur.attemptCount + 1 > cur.maxAttempts
    and {
      claimable(v, a, tnow),
      if (reason == "attempt_cap") overCap else reason == "invalid_bounds" and not(overCap) and cur.invalidBounds,
    }
  }
  action TW1_Error(r: int, a: int, reason: str): bool = {
    val tw = proc.tw1.get(r)
    val inflight = tw.phase != "idle"
    val v = if (inflight) overlay(db, tw) else db
    val tnow = if (inflight) tw.now else now
    all {
      RUNTIMES.contains(r), ACT_IDS.contains(a),
      if (inflight) tw.phase == "claim" and tw.cands.contains(a) and hereOn(tw, tw.topic) < tw.wanted
      else proc.topicHolder == 0,
      claimErrorRow(v, a, tnow, reason),
      commit(db, proc.with("topicHolder", if (proc.topicHolder == r) 0 else proc.topicHolder).with("tw1", proc.tw1.set(r, idleTw1)),
             ghost.with("claimAborted", true).with("taskErrors", ghost.taskErrors.setBy(r, x => x + 1))
                  .with("dispErrors", ghost.dispErrors.setBy(r, x => x + 1)), "TW1_Error"),
    }
  }

  //------------------------------------------------------ activity execution
  // executions are addressed by (runtime, activity, lease token); heartbeats by their id
  pure def isExec(e: Exec, r: int, a: int, tok: int): bool = e.rt == r and e.act == a and e.token == tok
  pure def findExec(p: Proc, r: int, a: int, tok: int): Exec = p.execs.fold(noExec, (acc, e) => if (isExec(e, r, a, tok)) e else acc)
  pure def hasExec(p: Proc, r: int, a: int, tok: int): bool = p.execs.exists(e => isExec(e, r, a, tok))
  pure def findHb(p: Proc, hb: int): Hb = p.hbs.fold(noHb, (acc, h) => if (h.id == hb) h else acc)
  pure def hasHb(p: Proc, hb: int): bool = p.hbs.exists(h => h.id == hb)
  pure def runningExecOf(p: Proc, h: Hb): Set[Exec] = p.execs.filter(e => isExec(e, h.rt, h.act, h.token) and e.phase == "running")

  // L-W: handler returns (or times out -> retryable) before the local deadline
  action HandlerReturn(r: int, a: int, tok: int, outcome: str): bool = {
    val e = findExec(proc, r, a, tok)
    all {
      hasExec(proc, r, a, tok),
      e.phase == "running",
      Set("succeeded", "retryable", "permanent").contains(outcome),
      now < e.deadline,
      commit(db, proc.with("execs", proc.execs.exclude(Set(e)).union(Set(e.with("phase", outcome)))), ghost, "HandlerReturn"),
    }
  }

  // L-W: local lease deadline passes -> cancel handler, return error without T-W3
  action LocalDeadline(r: int, a: int, tok: int): bool = {
    val e = findExec(proc, r, a, tok)
    all {
      hasExec(proc, r, a, tok),
      e.phase == "running",
      now >= e.deadline,
      commit(db, proc.with("execs", proc.execs.exclude(Set(e))), ghost, "LocalDeadline"),
    }
  }

  // T-W2 heartbeat hb (hb = ghost.nextHb) starts for execution (r, a, tok); its DB `now`
  // sample and COMMIT come later (TW2_Commit), or it is dropped
  action TW2_Send(r: int, a: int, tok: int, hb: int): bool = {
    val e = findExec(proc, r, a, tok)
    all {
      hasExec(proc, r, a, tok),
      e.phase == "running",
      now < e.deadline,
      not(proc.hbs.exists(h => h.rt == r and h.act == a and h.token == tok)),
      hb == ghost.nextHb,
      commit(db, proc.with("hbs", proc.hbs.union(Set({ id: hb, rt: r, act: a, attempt: e.attempt, token: tok, sentAt: now }))),
             ghost.with("nextHb", hb + 1), "TW2_Send"),
    }
  }

  pure def hbFenceOk(d: Db, h: Hb): bool = {
    val ar = d.act.get(h.act)
    ar.status == "running" and ar.attemptCount == h.attempt and ar.token == h.token and attOpenWith(d, h.act, h.attempt, h.token)
  }

  // T-W2 commit: DB now = sample, lease expiry leaseExp (> sample). Fenced on status=running,
  // attemptCount, leaseToken and the open attempt; NO expiry check (S10, G7). The UPDATE
  // blocks while an in-flight T-W1 holds the activity row.
  action TW2_Commit(hb: int, sample: int, leaseExp: int): bool = {
    val h = findHb(proc, hb)
    val ar = db.act.get(h.act)
    val alive = proc.execs.filter(e => isExec(e, h.rt, h.act, h.token) and now < e.deadline)
    val others = proc.execs.filter(e => not(isExec(e, h.rt, h.act, h.token)))
    all {
      hasHb(proc, hb),
      not(actLocked(proc, h.act)),
      hbFenceOk(db, h),
      leaseExp > sample,
      commitAt(setAct(db, h.act, ar.with("leaseExp", leaseExp)),
               proc.with("hbs", proc.hbs.exclude(Set(h)))
                   .with("execs", others.union(alive.map(e => e.with("deadline", leaseExp - 1 + DRIFT)))),
               ghost.with("revivedExpiredLease", ghost.revivedExpiredLease or (ar.leaseExp <= now and alive.size() == 0))
                    .with("staleActCommit", ghost.staleActCommit or h.token != ghost.lastIssuedAct.get(h.act)),
               "TW2_Commit", sample),
    }
  }

  // T-W2 fence miss: rollback, heartbeat failure cancels the handler (no T-W3)
  action TW2_FenceMiss(hb: int): bool = {
    val h = findHb(proc, hb)
    all {
      hasHb(proc, hb),
      not(actLocked(proc, h.act)),
      not(hbFenceOk(db, h)),
      commit(db, proc.with("hbs", proc.hbs.exclude(Set(h))).with("execs", proc.execs.exclude(runningExecOf(proc, h))),
             ghost, "TW2_FenceMiss"),
    }
  }

  // T-W2 future dropped before COMMIT (local deadline race, crash): rolled back
  action TW2_Drop(hb: int): bool = {
    val h = findHb(proc, hb)
    all {
      hasHb(proc, hb),
      commit(db, proc.with("hbs", proc.hbs.exclude(Set(h))).with("execs", proc.execs.exclude(runningExecOf(proc, h))),
             ghost, "TW2_Drop"),
    }
  }

  pure def actFenceOk(d: Db, e: Exec): bool = {
    val ar = d.act.get(e.act)
    ar.status == "running" and ar.attemptCount == e.attempt and ar.token == e.token and attOpenWith(d, e.act, e.attempt, e.token)
  }

  // T-W3 finish of execution (r, a, tok) with the handler's outcome: succeeded -> workflow
  // ready; retryable below the cap -> pending at availableAt (>= tnow); else dead-lettered and
  // the workflow blocked. Locks the activity, then the workflow: blocks on T-W1's locks.
  action TW3_Finish(r: int, a: int, tok: int, outcome: str, availableAt: int, tnow: int): bool = {
    val e = findExec(proc, r, a, tok)
    val ar = db.act.get(a)
    val w = ar.wf
    val wr = db.wf.get(w)
    val p1 = proc.with("execs", proc.execs.exclude(Set(e)))
    val g1 = ghost.with("staleActCommit", ghost.staleActCommit or tok != ghost.lastIssuedAct.get(a))
    val d0 = closeAttempt(db, a)
    all {
      hasExec(proc, r, a, tok),
      e.phase == outcome, outcome != "running",
      not(actLocked(proc, a)), not(wfLocked(proc, w)),
      actFenceOk(db, e),
      if (outcome == "succeeded") {
        val d1 = setAct(d0, a, ar.with("status", "succeeded").with("token", 0).with("leaseExp", 0))
        all {
          waitsOnAct(d1, w, a), canAppend(d1, w),
          commitAt(setWf(appendDeliverable(d1, w, "activity_succeeded", a), w,
                         wr.with("status", "ready").with("waitKind", "").with("waitRef", 0).with("availableAt", tnow)),
                   p1, g1, "TW3_Succeeded", tnow),
        }
      } else if (outcome == "retryable" and e.attempt < e.maxAttempts) all {
        availableAt >= tnow,
        commitAt(setAct(d0, a, ar.with("status", "pending").with("availableAt", availableAt).with("token", 0).with("leaseExp", 0)),
                 p1, g1, "TW3_Retry", tnow),
      } else {
        val d1 = setAct(d0, a, ar.with("status", "dead_lettered").with("token", 0).with("leaseExp", 0))
        all { waitsOnAct(d1, w, a), commitAt(setWf(d1, w, wr.with("status", "blocked")), p1, g1, "TW3_DeadLetter", tnow) }
      },
    }
  }

  // T-W3 rollback: fence miss, or success/dead-letter without the workflow wait
  action TW3_FenceMiss(r: int, a: int, tok: int): bool = {
    val e = findExec(proc, r, a, tok)
    val w = db.act.get(a).wf
    all {
      hasExec(proc, r, a, tok),
      e.phase != "running",
      not(actLocked(proc, a)), not(wfLocked(proc, w)),
      not(actFenceOk(db, e)) or ((e.phase == "succeeded" or e.phase == "permanent" or e.attempt >= e.maxAttempts)
                                 and not(waitsOnAct(db, w, a))),
      commit(db, proc.with("execs", proc.execs.exclude(Set(e))), ghost, "TW3_FenceMiss"),
    }
  }

  //------------------------------------------------ operator (admin/control.rs)
  // Each locks the workflow FOR UPDATE first; a rejected request (Conflict) is not a step.
  // A running row's lease is cleared: its coordinator's T-C2/T-C3 misses the fence (G1);
  // ghost.opRevoked remembers the revoked token.
  pure def opRevoke(g: Ghost, d: Db, w: int): Ghost =
    if (d.wf.get(w).status == "running") g.with("opRevoked", g.opRevoked.set(w, d.wf.get(w).token)) else g

  // pause_activity: the workflow waits on a running activity
  pure def pausesActivity(d: Db, w: int): bool = {
    val r = d.wf.get(w)
    r.waitKind == "activity" and ACT_IDS.contains(r.waitRef) and d.act.get(r.waitRef).status == "running"
  }

  // T-A2 pause_workflow: reject paused/terminal. If waiting on a running activity,
  // pause_activity (activity FOR UPDATE) closes its attempt (operator_paused) and sets it
  // pending at availableAt = tnow with maxAttempts + 1 and the lease cleared (S36); the handler
  // keeps running until its heartbeat or finish misses the fence (N2). Fenced workflow ->
  // paused with the lease cleared; wait fields and availableAt are kept.
  action AdminPause(w: int, tnow: int): bool = {
    val r = db.wf.get(w)
    val a = r.waitRef
    val bump = pausesActivity(db, w)
    val ar = if (ACT_IDS.contains(a)) db.act.get(a) else emptyAct
    val d1 = if (bump) setAct(closeAttempt(db, a), a, ar.with("status", "pending").with("availableAt", tnow)
                                                        .with("maxAttempts", ar.maxAttempts + 1)
                                                        .with("token", 0).with("leaseExp", 0))
             else db
    all {
      WF_IDS.contains(w), r.status != "none", r.status != "paused", not(isTerminal(r.status)),
      not(wfLocked(proc, w)),
      (r.waitKind == "activity" and ACT_IDS.contains(a)) implies not(actLocked(proc, a)),
      bump implies ar.maxAttempts + 1 <= MAX_ATTEMPTS,                 // sizing bound, not a code check
      commitAt(setWf(d1, w, r.with("status", "paused").with("token", 0).with("leaseExp", 0)), proc,
               opRevoke(ghost, db, w), if (bump) "AdminPauseActivity" else "AdminPause", tnow),
    }
  }

  // resume_status: the status a paused row resumes to, from its wait; "conflict" = rejected
  pure def resumeStatus(d: Db, w: int): str = {
    val r = d.wf.get(w)
    if (r.waitKind == "") "ready"
    else if (r.waitKind == "activity") {
      val s = if (ACT_IDS.contains(r.waitRef)) d.act.get(r.waitRef).status else "none"
      if (s == "pending" or s == "running") "waiting_activity"
      else if (s == "dead_lettered") "blocked"
      else if (s == "succeeded") "ready"
      else "conflict"
    }
    else if (r.waitKind == "child" and WF_IDS.contains(r.waitRef) and d.wf.get(r.waitRef).status != "none")
      if (isTerminal(d.wf.get(r.waitRef).status)) "conflict" else "waiting_child"
    else "conflict"
  }

  // T-A3 resume_workflow: paused -> the status derived from its wait (plain reads of the
  // activity / child); availableAt = tnow only when resuming to ready; wait fields kept
  action AdminResume(w: int, tnow: int): bool = {
    val r = db.wf.get(w)
    val s = resumeStatus(db, w)
    all {
      WF_IDS.contains(w), r.status == "paused", not(wfLocked(proc, w)), s != "conflict",
      commitAt(setWf(db, w, r.with("status", s).with("availableAt", if (s == "ready") tnow else r.availableAt)
                             .with("token", 0).with("leaseExp", 0)),
               proc, ghost, "AdminResume", tnow),
    }
  }

  // T-A4 cancel_workflow: terminal -> Conflict; else cancel_locked_workflow as T-X3. Children
  // are not cancelled (G11).
  action AdminCancel(w: int, tnow: int): bool = all {
    cancelOk(w),
    wakeParentsOk(cancelWrite(db, w), w),
    commitAt(wakeParents(cancelWrite(db, w), w, "child_cancelled", tnow), proc, opRevoke(ghost, db, w), "AdminCancel", tnow),
  }

  //-------------------------------------------------------------- environment
  // process crash: in-memory state lost, rows remain; an in-flight T-W1 rolls back and
  // releases its locks; heartbeat COMMITs already sent may still land (hbs are kept)
  // Crash(r) changes the state only when r holds process-local state; the crash of an
  // idle runtime is a stutter (a trace checker replays it as one).
  def crashLoses(r: int): bool =
    proc.claims.get(r).active or proc.execs.exists(e => e.rt == r) or proc.tw1.get(r).phase != "idle"

  action Crash(r: int): bool = all {
    ENABLE_CRASH,
    RUNTIMES.contains(r),
    crashLoses(r),
    commit(db, proc.with("claims", proc.claims.set(r, noClaim))
                   .with("execs", proc.execs.filter(e => e.rt != r))
                   .with("tw1", proc.tw1.set(r, idleTw1))
                   .with("topicHolder", if (proc.topicHolder == r) 0 else proc.topicHolder),
           ghost.with("coordErrors", ghost.coordErrors.set(r, 0)).with("dispErrors", ghost.dispErrors.set(r, 0)), "Crash"),
  }

  // external write (raw SQL, as the G10 gap test does): activity a's timeout/lease bounds
  // become invalid. Blocks while an in-flight T-W1 holds the row.
  action EnvCorruptActivityBounds(a: int): bool = all {
    ENABLE_ENV_EDITS,
    ACT_IDS.contains(a), db.act.get(a).status != "none", not(actLocked(proc, a)),
    commit(setAct(db, a, db.act.get(a).with("invalidBounds", true)), proc, ghost, "EnvCorruptActivityBounds"),
  }

  // Recorded external writes (trace replay only; not part of step). Each sets one row to
  // the recorded image. Ids and tokens it mentions count as issued (the counters move past
  // them), and a non-zero lease token becomes the row's latest issued lease, so a holder of
  // an older token that still commits is a stale commit (S2/S10). Blocks while an
  // in-flight T-W1 holds the row.
  pure def envIssue(d: Db, tok: int): Db = d.with("nextToken", max2(d.nextToken, tok + 1))
  action EnvSetWf(w: int, row: WfRow): bool = all {
    ENABLE_ENV_EDITS, WF_IDS.contains(w), not(wfLocked(proc, w)),
    commit(envIssue(setWf(db, w, row), row.token).with("nextWf", max2(db.nextWf, w + 1)), proc,
           if (row.token != 0) ghost.with("lastIssuedWf", ghost.lastIssuedWf.set(w, row.token)) else ghost,
           "EnvSetWf"),
  }
  action EnvSetAct(a: int, row: ActRow): bool = all {
    ENABLE_ENV_EDITS, ACT_IDS.contains(a), not(actLocked(proc, a)),
    commit(envIssue(setAct(db, a, row), row.token).with("nextAct", max2(db.nextAct, a + 1)), proc,
           if (row.token != 0) ghost.with("lastIssuedAct", ghost.lastIssuedAct.set(a, row.token)) else ghost,
           "EnvSetAct"),
  }
  action EnvSetAtt(a: int, n: int, row: AttRow): bool = all {
    ENABLE_ENV_EDITS, ACT_IDS.contains(a), ATT_NOS.contains(n), not(actLocked(proc, a)),
    commit(envIssue(setAtt(db, a, n, row), row.token), proc, ghost, "EnvSetAtt"),
  }
  action EnvAppendEvent(w: int, ev: Event): bool = all {
    ENABLE_ENV_EDITS, WF_IDS.contains(w), not(wfLocked(proc, w)),
    commit(db.with("events", db.events.set(w, db.events.get(w).union(Set(ev)))), proc, ghost, "EnvAppendEvent"),
  }

  // simulation only: DB time advances by one tick
  action Tick: bool = all {
    now < MAX_TIME,
    db' = db, proc' = proc, ghost' = ghost, now' = now + 1, lastAction' = "Tick",
  }

  //--------------------------------------------------------------- init/step
  action init: bool = all {
    now' = 0,
    db' = {
      wf: WF_IDS.mapBy(w => emptyWf),
      act: ACT_IDS.mapBy(a => emptyAct),
      att: ACT_IDS.mapBy(a => ATT_NOS.mapBy(n => noAtt)),
      events: WF_IDS.mapBy(w => Set()),
      nextToken: 1, nextWf: 1, nextAct: 1,
    },
    proc' = {
      claims: RUNTIMES.mapBy(r => noClaim),
      execs: Set(), hbs: Set(),
      tw1: RUNTIMES.mapBy(r => idleTw1),
      topicHolder: 0,
    },
    ghost' = {
      staleWfCommit: false, staleActCommit: false, capExceededAtClaim: false,
      tx2OutsideLineage: false, claimAborted: false, noEventError: false, revivedExpiredLease: false,
      nextHb: 1,
      lastIssuedWf: WF_IDS.mapBy(w => 0), lastIssuedAct: ACT_IDS.mapBy(a => 0), taskErrors: RUNTIMES.mapBy(r => 0),
      coordErrors: RUNTIMES.mapBy(r => 0), dispErrors: RUNTIMES.mapBy(r => 0),
      opRevoked: WF_IDS.mapBy(w => 0), opTaskErrors: RUNTIMES.mapBy(r => 0),
    },
    prevWf' = WF_IDS.mapBy(w => "none"),
    prevAct' = ACT_IDS.mapBy(a => "none"),
    lastAction' = "init",
  }

  // record the pre-state statuses for the action properties S6 and S14
  action keepPrev: bool = all {
    prevWf' = WF_IDS.mapBy(x => db.wf.get(x).status),
    prevAct' = ACT_IDS.mapBy(x => db.act.get(x).status),
  }

  // reconcile's new available_at for row a in r's in-flight T-W1, given a retry delay
  def reconcileAvail(r: int, a: int, delay: int): int = {
    val tw = proc.tw1.get(r)
    if (ACT_IDS.contains(a) and reconcileExhausted(overlay(db, tw), a)) tw.now else tw.now + delay
  }

  action step: bool = {
    nondet r = RUNTIMES.oneOf()
    nondet w = WF_IDS.oneOf()
    // state-guided choices (a superset of the enabled ones; 0 = none)
    nondet w0 = WF_IDS.filter(x => db.wf.get(x).status == "running" and db.wf.get(x).leaseExp <= now).union(Set(0)).oneOf()
    nondet w1 = { val due = WF_IDS.filter(x => db.wf.get(x).status == "ready" and db.wf.get(x).availableAt <= now)
                  if (due == Set()) Set(0) else due }.oneOf()
    nondet key = Set(NO_KEY, DOMAIN_KEY).oneOf()
    nondet from = WF_IDS.filter(x => db.wf.get(x).status != "none").union(Set(0)).oneOf()
    nondet inserted = Set(true, false).oneOf()
    nondet kind = Set("P", "C").oneOf()
    nondet superseded = Set(true, false).oneOf()
    nondet childKey = Set("domain", "auto").oneOf()
    nondet topic = TOPICS.oneOf()
    nondet backoff = Set(0, 1).oneOf()
    nondet outcome = Set("succeeded", "retryable", "permanent").oneOf()
    nondet tnowOne = Set(now, if (now > 0) now - 1 else 0).oneOf()
    nondet badBounds = Set(false, ENABLE_ENV_EDITS).oneOf()
    nondet prio = Set(false, true).oneOf()
    nondet maxA = Set(max2(1, MAX_ATTEMPTS - 1), MAX_ATTEMPTS).oneOf()   // < MAX_ATTEMPTS leaves room for a pause bump
    nondet ea = ACT_IDS.oneOf()
    nondet a = { val c = proc.tw1.get(r).cands
                 if (c == Set()) Set(0) else c }.oneOf()
    // dummy elements keep oneOf() total; every action guards on membership
    nondet e = (if (proc.execs == Set()) Set(noExec) else proc.execs).oneOf()
    nondet h = (if (proc.hbs == Set()) Set(noHb) else proc.hbs).oneOf()
    // values derived from the state (a trace supplies them instead)
    val c = proc.claims.get(r)
    val cw = c.wf
    val cmd = if (cw == 0) 0 else db.wf.get(cw).cmdSeq + 1
    val ck = if (childKey == "domain") DOMAIN_KEY else autoKey(cw, cmd)
    val cEx = anyOf(rowsWithKey(db, "C", ck))
    val tx1W = if (inserted) db.nextWf else if (key != NO_KEY) anyOf(rowsWithKey(db, "P", key)) else 0
    val o2 = tx2Orig(db, kind, DOMAIN_KEY)
    val s2 = if (o2 == 0 or superseded) db.nextWf else 0
    all {
      keepPrev,
      any {
        TX1_Start(tx1W, "P", key, if (key == NO_KEY) from else 0, inserted, now),
        TX2_RecoverableStart(kind, DOMAIN_KEY, o2, tx2Latest(db, kind, DOMAIN_KEY), superseded, s2, now),
        TX3_Cancel(w, now),
        TC1_Claim(r, w0, w1, db.nextToken, now + LEASE_W, now),
        LC1_NoEvent(r, cw),
        CoordFenceMiss(r, cw, c.token),
        TC2_Continue(r, cw, c.token, now + backoff, now),
        TC2_Complete(r, cw, c.token, now),
        TC2_RunActivity(r, cw, c.token, db.nextAct, topic, maxA, badBounds, prio, if (prio) CONTINUATION_READY_AT else now, now),
        TC2_RunChild(r, cw, c.token, "C", ck, cEx, if (cEx == 0) db.nextWf else 0, now),
        TC3_ActivationFailure(r, cw, c.token, if (cw == 0) 0 else db.wf.get(cw).actAttempts + 1, MAX_ACTIVATION, now + backoff, now),
        TW1_BeginBatch(r),
        TW1_BeginOne(r, topic, tnowOne),
        TW1_ReconcileScan(r, topic),
        TW1_ReconcileRow(r, a, reconcileAvail(r, a, backoff)),
        TW1_Count(r),
        TW1_ClaimRow(r, a, db.nextToken),
        TW1_SkipRow(r, a),
        TW1_Commit(r),
        TW1_Error(r, a, "attempt_cap"),
        TW1_Error(r, a, "invalid_bounds"),
        EnvCorruptActivityBounds(ea),
        AdminPause(w, now),
        AdminResume(w, now),
        AdminCancel(w, now),
        HandlerReturn(e.rt, e.act, e.token, outcome),
        LocalDeadline(e.rt, e.act, e.token),
        TW2_Send(e.rt, e.act, e.token, ghost.nextHb),
        TW2_Commit(h.id, h.sentAt, h.sentAt + LEASE_A),
        TW2_FenceMiss(h.id),
        TW2_Drop(h.id),
        TW3_Finish(e.rt, e.act, e.token, e.phase, now + backoff, now),
        TW3_FenceMiss(e.rt, e.act, e.token),
        Crash(r),
        Tick,
      },
    }
  }

  //=========================================================== INVARIANTS
  // S1: running <=> lease token and expiry set
  val inv_S1_wfLease = WF_IDS.forall(w => {
    val r = db.wf.get(w)
    (r.status == "running") iff (r.token != 0 and r.leaseExp != 0)
  })

  // S2: every committed T-C2/T-C3 used the most recently issued token for that row
  val inv_S2_noStaleWfCommit = not(ghost.staleWfCommit)

  // S5: delivery-sequence discipline
  val inv_S5_delivery = WF_IDS.forall(w => {
    val r = db.wf.get(w)
    val und = db.events.get(w).filter(e => e.dseq > r.delivered)
    and {
      und.size() <= 1,                                                              // S5c
      (r.status == "ready" or r.status == "running") implies und.exists(e => e.dseq == r.delivered + 1), // S5d
      Set("waiting_activity", "waiting_child", "blocked").contains(r.status) implies und.size() == 0,
      db.events.get(w).forall(e1 => db.events.get(w).forall(e2 => e1.dseq == e2.dseq implies e1 == e2)), // S5a
    }
  })
  val inv_S5_noEventError = not(ghost.noEventError)

  // S6: terminal workflow statuses are absorbing (action property via prevWf)
  val inv_S6_wfTerminalAbsorbing = WF_IDS.forall(w => isTerminal(prevWf.get(w)) implies db.wf.get(w).status == prevWf.get(w))

  // S7: wait coupling for non-terminal rows
  val inv_S7_waitCoupling = WF_IDS.forall(w => {
    val r = db.wf.get(w)
    and {
      (r.status == "ready" or r.status == "running") implies (r.waitKind == "" and r.waitRef == 0),
      (r.status == "waiting_activity" or r.status == "blocked") implies (r.waitKind == "activity" and r.waitRef != 0),
      r.status == "waiting_child" implies (r.waitKind == "child" and r.waitRef != 0),
    }
  })

  // S8: activation attempts bounded
  val inv_S8_activationBounded = WF_IDS.forall(w => db.wf.get(w).actAttempts <= MAX_ACTIVATION)

  // S9: running activity <=> token and expiry set; one current executor per token
  val inv_S9_actLease = ACT_IDS.forall(a => {
    val r = db.act.get(a)
    and {
      (r.status == "running") iff (r.token != 0 and r.leaseExp != 0),
      proc.execs.filter(e => e.act == a and e.token == r.token and r.token != 0).size() <= 1,
    }
  })

  // S10: no committed heartbeat/finish with a stale token
  val inv_S10_noStaleActCommit = not(ghost.staleActCommit)

  // S11: running <=> exactly one open attempt, the current one, with the same token
  val inv_S11_openAttempt = ACT_IDS.forall(a => {
    val r = db.act.get(a)
    val opens = ATT_NOS.filter(n => db.att.get(a).get(n).open)
    if (r.status == "running") opens == Set(r.attemptCount) and db.att.get(a).get(r.attemptCount).token == r.token
    else opens.size() == 0
  })

  // S12: attempts contiguous from 1 and bounded by maxAttempts
  val inv_S12_attemptsBounded = ACT_IDS.forall(a => {
    val r = db.act.get(a)
    and {
      r.status != "none" implies r.attemptCount <= r.maxAttempts,
      ATT_NOS.forall(n => db.att.get(a).get(n).used iff n <= r.attemptCount),
    }
  })

  // S13: at most one handler executes per activity (executing = running phase, before local deadline)
  def executing(e: Exec): bool = e.phase == "running" and now < e.deadline
  val inv_S13_oneHandler = ACT_IDS.forall(a => proc.execs.filter(e => e.act == a and executing(e)).size() <= 1)
  // N2: "real concurrency stays within the topic cap" (INVARIANTS.md G7). Violated without G7:
  // cancel frees the slot at once, the cancelled handler runs until its heartbeat fence miss (L12).
  val inv_S13_topicConcurrency = TOPICS.forall(t =>
    proc.execs.filter(e => executing(e) and db.act.get(e.act).topic == t).size() <= TOPIC_CAP.get(t))

  // S14: activity terminal states
  val inv_S14_actTerminal = ACT_IDS.forall(a => {
    val p = prevAct.get(a)
    val s = db.act.get(a).status
    and {
      (p == "succeeded" or p == "cancelled") implies s == p,
      p == "dead_lettered" implies (s == p or s == "cancelled"),
    }
  })

  // S15: activity/workflow coupling; a paused workflow keeps its wait on a pending activity
  pure def pausedOnAct(d: Db, w: int, a: int): bool =
    d.wf.get(w).status == "paused" and d.wf.get(w).waitKind == "activity" and d.wf.get(w).waitRef == a
  val inv_S15_actWfCoupling = and {
    ACT_IDS.forall(a => {
      val r = db.act.get(a)
      and {
        r.status == "running" implies waitsOnAct(db, r.wf, a),
        r.status == "pending" implies (waitsOnAct(db, r.wf, a) or pausedOnAct(db, r.wf, a)),
      }
    }),
    WF_IDS.forall(w => db.wf.get(w).status == "blocked" implies db.act.get(db.wf.get(w).waitRef).status == "dead_lettered"),
  }

  // S16: activity success delivered at most once
  val inv_S16_successOnce = ACT_IDS.forall(a =>
    WF_IDS.map(w => db.events.get(w).filter(e => e.typ == "activity_succeeded" and e.ref == a).size())
      .fold(0, (s, n) => s + n) <= 1)

  // S17 as stated: at each T-W1 commit, live leases on a topic <= cap
  val inv_S17_capAtClaim = not(ghost.capExceededAtClaim)
  // S17 strengthened to every state (the G7 gap is about this form)
  val inv_S17_capAlways = TOPICS.forall(t => liveOnTopic(db, t, now) <= TOPIC_CAP.get(t))

  // S18: at most one row per (kind, dedupKey)
  val inv_S18_dedup = WF_IDS.forall(x => WF_IDS.forall(y =>
    (x != y and db.wf.get(x).status != "none" and db.wf.get(y).status != "none" and db.wf.get(x).dedup != NO_KEY)
      implies not(db.wf.get(x).kind == db.wf.get(y).kind and db.wf.get(x).dedup == db.wf.get(y).dedup)))

  // S19 (as in INVARIANTS.md): at most one successor per source (unique restart key)
  val inv_S19_successor = WF_IDS.forall(x => {
    val src = db.wf.get(x).restartedFrom
    src != 0 implies WF_IDS.forall(y => (y != x) implies db.wf.get(y).restartedFrom != src)
  })
  // N3 (stronger form): the source of a successor is terminal. T-X2 and admin restart only
  // restart terminal rows; a public StartOptions.restarted_from_workflow_id is not checked.
  val inv_S19_sourceTerminal = WF_IDS.forall(x => {
    val src = db.wf.get(x).restartedFrom
    src != 0 implies isTerminal(db.wf.get(src).status)
  })

  // S23: a child outcome reaches each parent at most once per wait (parent command). A parent
  // that awaits the same keyed child in two commands legitimately receives it twice.
  val inv_S23_childOnce = WF_IDS.forall(p => WF_IDS.forall(c => db.events.get(p).forall(e1 =>
    db.events.get(p).filter(e => e.ref == c and e.cmd == e1.cmd
      and Set("child_succeeded", "child_failed", "child_cancelled").contains(e.typ)).size() <= 1)))

  // S24: a parent waiting on a child never waits on a terminal child (wake is in the child's terminal txn)
  val inv_S24_parentWakes = WF_IDS.forall(p => {
    val r = db.wf.get(p)
    r.status == "waiting_child" implies not(isTerminal(db.wf.get(r.waitRef).status))
  })

  // S24 in the scope INVARIANTS.md claims (every terminal transition except T-X2 supersession)
  val inv_S24_exceptTX2 = WF_IDS.forall(p => {
    val r = db.wf.get(p)
    r.status == "waiting_child" implies
      (not(isTerminal(db.wf.get(r.waitRef).status)) or WF_IDS.exists(y => db.wf.get(y).restartedFrom == r.waitRef))
  })

  // S25: a cancelled workflow has no pending/running activity
  val inv_S25_cancelAtomic = ACT_IDS.forall(a => {
    val r = db.act.get(a)
    (r.status == "pending" or r.status == "running") implies db.wf.get(r.wf).status != "cancelled"
  })

  // G10 (spec gap): a T-W1 never aborts because of one row (TW1_Error rolls back every
  // topic's claims). Violated once a claimable row has invalid bounds (ENABLE_ENV_EDITS).
  val inv_G10_noClaimAbort = not(ghost.claimAborted)

  // G11 (spec gap): cancelling a parent reaches its children
  val inv_G11_cancelReachesChildren = WF_IDS.forall(p => db.wf.get(p).status == "cancelled" implies
    WF_IDS.forall(c => (db.wf.get(c).parent == p and db.wf.get(c).status != "none") implies isTerminal(db.wf.get(c).status)))

  // N1: T-X2 supersedes or returns only a row on the keyed row's own restart chain
  val inv_N1_tx2OwnLineage = not(ghost.tx2OutsideLineage)

  // G1 (spec gap): an operator action never costs a runtime its restart budget. A pause or
  // cancel of a running row makes its coordinator's T-C2/T-C3 miss the fence; FencedWrite is a
  // task error that counts toward MAX_TASK_RESTARTS and never resets, so enough of them
  // self-cancel the runtime (stoppedRt). Stated in its strong form: no operator-caused task error.
  val inv_G1_noSelfCancelFromOperator = RUNTIMES.forall(r => ghost.opTaskErrors.get(r) == 0)

  // conjunction of the invariants expected to hold. Checked separately: S17 (G7 under the
  // historical RR snapshot), S24 unrestricted (G2), S13 per topic (N2), S19 source terminal (N3),
  // G1, G10, G11, N1.
  val safety = and {
    inv_S1_wfLease, inv_S2_noStaleWfCommit, inv_S5_delivery, inv_S5_noEventError, inv_S6_wfTerminalAbsorbing,
    inv_S7_waitCoupling, inv_S8_activationBounded, inv_S9_actLease, inv_S10_noStaleActCommit,
    inv_S11_openAttempt, inv_S12_attemptsBounded, inv_S13_oneHandler, inv_S14_actTerminal,
    inv_S15_actWfCoupling, inv_S16_successOnce, inv_S18_dedup, inv_S19_successor,
    inv_S23_childOnce, inv_S24_exceptTX2, inv_S25_cancelAtomic,
  }
  // safety plus S17 (expected to hold under READ COMMITTED)
  val safetyRc = safety and inv_S17_capAtClaim and inv_S17_capAlways

  //============================================ WITNESSES (expected to fail)
  // Each is the negation of a reachable interesting state; a violation proves reachability.
  val wit_S3_concurrentStep = not(RUNTIMES.exists(r1 => RUNTIMES.exists(r2 =>
    r1 != r2 and proc.claims.get(r1).active and proc.claims.get(r2).active and proc.claims.get(r1).wf == proc.claims.get(r2).wf)))
  val wit_blocked = not(WF_IDS.exists(w => db.wf.get(w).status == "blocked"))
  val wit_childSucceeded = not(WF_IDS.exists(w => db.events.get(w).exists(e => e.typ == "child_succeeded")))
  val wit_activitySucceeded = not(WF_IDS.exists(w => db.events.get(w).exists(e => e.typ == "activity_succeeded")))
  val wit_revivedLease = not(ghost.revivedExpiredLease)
  val wit_coordFenceMiss = RUNTIMES.forall(r => ghost.taskErrors.get(r) == 0)
  val wit_selfCancelled = RUNTIMES.forall(r => not(stoppedRt(ghost, r)))
  val wit_pausedActivity = not(ACT_IDS.exists(a => pausedOnAct(db, db.act.get(a).wf, a) and db.act.get(a).status == "pending"))
  val wit_reconciled = lastAction != "TW1_ReconcileRow"
  val wit_tw1Interleaved = not(RUNTIMES.exists(r => proc.tw1.get(r).phase != "idle")
                               and Set("TW2_Commit", "TX3_Cancel", "TC2_RunActivity", "TW3_Succeeded").contains(lastAction))

  //============================================================ LIVENESS
  // Temporal properties. Fairness assumptions are stated in each hypothesis.
  // F1 (DB time advances) = weak fairness of Tick; F2 (a runtime stays up) =
  // crashes are disabled (ENABLE_CRASH=false) or eventually stop; F4 (step and
  // handlers return) = weak fairness of TC2/HandlerReturn for the holder.
  def expiredWf(w: int): bool = db.wf.get(w).status == "running" and db.wf.get(w).leaseExp <= now
  def readyDue(w: int): bool = db.wf.get(w).status == "ready" and db.wf.get(w).availableAt <= now
  def expiredAct(a: int): bool = db.act.get(a).status == "running" and db.act.get(a).leaseExp <= now

  // Fairness: some coordinator recovers/claims whenever it can (F2, F3).
  temporal fairCoordinator = RUNTIMES.forall(r => WF_IDS.forall(w =>
    weakFair(TC1_Claim(r, w, 0, 0, 0, now), (db, proc)) and weakFair(TC1_Claim(r, 0, w, db.nextToken, now + LEASE_W, now), (db, proc))))
  temporal fairDispatcher = RUNTIMES.forall(r => and {
    weakFair(TW1_BeginBatch(r), (db, proc)), weakFair(TW1_Count(r), (db, proc)), weakFair(TW1_Commit(r), (db, proc)),
    TOPICS.forall(t => weakFair(TW1_ReconcileScan(r, t), (db, proc))),
    ACT_IDS.forall(a => weakFair(TW1_ReconcileRow(r, a, reconcileAvail(r, a, 1)), (db, proc)) and weakFair(TW1_ClaimRow(r, a, db.nextToken), (db, proc))),
  })

  // L1: a due ready workflow is eventually claimed (or otherwise leaves ready)
  temporal L1_readyClaimed = fairCoordinator implies WF_IDS.forall(w => always(readyDue(w) implies eventually(db.wf.get(w).status != "ready")))
  // L2: an expired workflow lease is eventually recovered (or committed)
  temporal L2_expiredRecovered = fairCoordinator implies WF_IDS.forall(w => always(expiredWf(w) implies eventually(not(expiredWf(w)))))
  // L5: an expired activity lease is eventually reconciled (or revived by a late heartbeat)
  temporal L5_expiredReconciled = fairDispatcher implies ACT_IDS.forall(a => always(expiredAct(a) implies eventually(not(expiredAct(a)))))
}
