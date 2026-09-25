// Directed scenarios: each `run` replays a counterexample (or witness) step by step.
// Every step is one action with explicit parameters, as a trace checker calls it.
// Run with: npx quint test durable_tests.qnt --main durable_tests      (READ COMMITTED, the code)
//           npx quint test durable_tests.qnt --main durable_tests_rr   (historical REPEATABLE READ T-W1)
module durable_tests {
  import durable(
    RUNTIMES = Set(1, 2), MAX_WF = 4, MAX_ACT = 3, TOPICS = Set("t"), TOPIC_CAP = Map("t" -> 1),
    LOCAL_SLOTS = 1, MAX_ATTEMPTS = 2, MAX_ACTIVATION = 2, LEASE_W = 3, LEASE_A = 3, MAX_TIME = 10,
    DRIFT = 0, RR_SNAPSHOT = false, ENABLE_CHILDREN = true, ENABLE_CRASH = true, ENABLE_ENV_EDITS = false, MAX_TASK_RESTARTS = 1
  ).* from "./durable"

  // step helpers; values a trace would supply are read from the state here
  def cw(r: int): int = proc.claims.get(r).wf
  def ct(r: int): int = proc.claims.get(r).token
  def atok(a: int): int = db.act.get(a).token
  action tick: bool = all { keepPrev, Tick }
  action start(key: int): bool = all { keepPrev, TX1_Start(db.nextWf, "P", key, 0, true, now) }
  action claim(r: int, w: int): bool = all { keepPrev, TC1_Claim(r, 0, w, db.nextToken, now + LEASE_W, now) }
  action runAct(r: int): bool = all { keepPrev, TC2_RunActivity(r, cw(r), ct(r), db.nextAct, "t", MAX_ATTEMPTS, false, false, now, now) }
  action complete(r: int): bool = all { keepPrev, TC2_Complete(r, cw(r), ct(r), now) }
  action runChild(r: int, k: int): bool = {
    val ex = anyOf(rowsWithKey(db, "C", k))
    all { keepPrev, TC2_RunChild(r, cw(r), ct(r), "C", k, ex, if (ex == 0) db.nextWf else 0, now) }
  }
  def nextAutoKey(r: int): int = autoKey(cw(r), db.wf.get(cw(r)).cmdSeq + 1)
  action tx2(kind: str, superseded: bool): bool = {
    val o = tx2Orig(db, kind, DOMAIN_KEY)
    all { keepPrev, TX2_RecoverableStart(kind, DOMAIN_KEY, o, tx2Latest(db, kind, DOMAIN_KEY), superseded,
                                         if (o == 0 or superseded) db.nextWf else 0, now) }
  }
  action handler(r: int, a: int, outcome: str): bool = all { keepPrev, HandlerReturn(r, a, atok(a), outcome) }
  action finish(r: int, a: int, outcome: str): bool = all { keepPrev, TW3_Finish(r, a, atok(a), outcome, now, now) }

  // workflow started, claimed by r, committed to RunActivity (next activity id)
  action startAndRunActivity(r: int): bool = start(NO_KEY).then(claim(r, db.nextWf - 1)).then(runAct(r))

  // simulation form of a claim_batch with nothing to reconcile that claims activity a
  action tw1Claim(r: int, a: int): bool =
    all { keepPrev, TW1_BeginBatch(r) }
      .then(all { keepPrev, TW1_ReconcileScan(r, "t") })
      .then(all { keepPrev, TW1_Count(r) })
      .then(all { keepPrev, TW1_ClaimRow(r, a, db.nextToken) })
      .then(all { keepPrev, TW1_Commit(r) })
  // replay form: one committed T-W1 that saw inFlight live leases and claimed a
  action tw1Replay(r: int, a: int, inFlight: int): bool =
    all { keepPrev, TW1_Claim(r, now, Map("t" -> 1), [], Map("t" -> inFlight), [{ a: a, tok: db.nextToken, leaseExp: now + LEASE_A }]) }

  // r1 claims a1 (token 3) at t=0, sends heartbeat 1 at t=1, gives up locally at t=3;
  // r2 starts T-W1 at t=3 and its reconcile scan sees a1 expired (3 <= 3)
  action g7Prefix: bool =
    init
      .then(startAndRunActivity(1))                                    // w1 waits on a1 (tokens 1, 2 below)
      .then(startAndRunActivity(1))                                    // w2 waits on a2
      .then(tw1Claim(1, 1))                                            // a1 token 3, lease to 3
      .then(tick)                                                      // t=1
      .then(all { keepPrev, TW2_Send(1, 1, 3, 1) })                    // heartbeat 1 starts at t=1
      .then(tick).then(tick)                                           // t=3
      .then(all { keepPrev, LocalDeadline(1, 1, 3) })                  // local deadline 2 passed
      .then(all { keepPrev, TW1_BeginBatch(2) })
      .then(all { keepPrev, TW1_ReconcileScan(2, "t") })

  // G7 under READ COMMITTED: the late heartbeat lands between the reconcile scan and the
  // relock; the relock skips a1 and the in_flight count (a later statement) sees it live
  run g7ClosedUnderRcTest =
    g7Prefix
      .expect(proc.tw1.get(2).cands == Set(1))
      .then(all { keepPrev, TW2_Commit(1, 1, 4) })                     // sample 1, leaseExp 4
      .expect(not(wit_revivedLease))                                   // an expired lease with no executor revived
      .then(all { keepPrev, TW1_ReconcileRow(2, 1, 4) })
      .expect(lastAction == "TW1_ReconcileSkip")
      .then(all { keepPrev, TW1_Count(2) })
      .expect(lastAction == "TW1_CountFull")                           // wanted = 1 - 1 = 0
      .then(all { keepPrev, TW1_Commit(2) })
      .expect(inv_S17_capAlways and inv_S17_capAtClaim and proc.execs.size() == 0 and safety)

  // The other order: the relock reconciles a1 first (pending again after a 1-tick retry
  // delay, so not a candidate at t=3); the heartbeat blocks on the row lock and then misses
  // the fence
  run g7RcHeartbeatBlockedTest =
    g7Prefix
      .then(all { keepPrev, TW1_ReconcileRow(2, 1, 4) })
      .expect(lastAction == "TW1_ReconcileRow" and actLocked(proc, 1) and db.act.get(1).status == "running")
      .then(all { keepPrev, TW1_Count(2) })
      .expect(proc.tw1.get(2).wanted == 1 and proc.tw1.get(2).cands == Set(2))
      .then(all { keepPrev, TW1_ClaimRow(2, 2, 4) })
      .then(all { keepPrev, TW1_Commit(2) })
      .expect(db.act.get(1).status == "pending" and db.act.get(1).availableAt == 4 and db.act.get(2).status == "running")
      .then(all { keepPrev, TW2_FenceMiss(1) })
      .expect(inv_S17_capAlways and inv_S17_capAtClaim and safety)

  // Replay: a recorded T-W1 that under-counted in_flight is flagged by inv_S17_capAtClaim
  run tw1ReplayDetectsCapTest =
    init
      .then(startAndRunActivity(1))
      .then(startAndRunActivity(1))
      .then(tw1Replay(1, 1, 0))
      .expect(lastAction == "TW1_Claim" and inv_S17_capAtClaim)
      .then(tw1Replay(2, 2, 0))                                        // saw 0 live leases, a1 is live
      .expect(not(inv_S17_capAtClaim) and not(inv_S17_capAlways))

  // Replay: a reconciled row that goes back to pending carries the retry delay
  // (availableAt >= tnow); a dead-lettered one carries tnow exactly
  action expiredA1: bool =
    init.then(startAndRunActivity(1)).then(tw1Replay(1, 1, 0)).then(tick).then(tick).then(tick)   // a1 lease 3, t=3
  run reconcileReplayDelayTest =
    expiredA1
      .then(all { keepPrev, TW1_Claim(2, 3, Map("t" -> 1), [{ a: 1, exhausted: false, availableAt: 5 }], Map("t" -> 0), []) })
      .expect(db.act.get(1).status == "pending" and db.act.get(1).availableAt == 5 and safety)
  run reconcileReplayEarlyRejectedTest =
    expiredA1
      .then(all { keepPrev, TW1_Claim(2, 3, Map("t" -> 1), [{ a: 1, exhausted: false, availableAt: 2 }], Map("t" -> 0), []) })
      .fail()

  // N2: cancel frees the topic slot while the cancelled handler still executes
  run n2CancelFreesSlotTest =
    init
      .then(startAndRunActivity(1))
      .then(startAndRunActivity(1))
      .then(tw1Claim(1, 1))                                            // r1 runs a1, cap 1 reached
      .then(all { keepPrev, TX3_Cancel(1, now) })                      // app cancels w1
      .then(tw1Claim(2, 2))                                            // r2 claims a2
      .expect(proc.execs.size() == 2 and not(inv_S13_topicConcurrency) and safety and inv_S17_capAlways)

  // P (w1) spawns keyed child C (w2); C's activity dead-letters -> C blocked
  action blockedKeyedChild: bool =
    init
      .then(start(NO_KEY))
      .then(claim(1, 1))
      .then(runChild(1, DOMAIN_KEY))                                   // w2 = C, key "k", root w1
      .then(claim(1, 2))
      .then(runAct(1))                                                 // a1 for w2
      .then(tw1Claim(1, 1))
      .then(handler(1, 1, "permanent"))
      .then(finish(1, 1, "permanent"))

  // G2: T-X2 cancels the blocked child without waking its parent
  run g2StrandedParentTest =
    blockedKeyedChild
      .expect(db.wf.get(2).status == "blocked" and db.wf.get(1).status == "waiting_child")
      .then(tx2("C", true))
      .expect(db.wf.get(2).status == "cancelled" and db.wf.get(3).restartedFrom == 2
              and not(inv_S24_parentWakes) and inv_N1_tx2OwnLineage and safety)

  // N1: the keyed child succeeded; a later auto-keyed sibling of the same kind is blocked.
  // T-X2(C, "k") resolves root = parent's root and supersedes the SIBLING.
  run n1WrongLineageTest =
    init
      .then(start(NO_KEY))
      .then(claim(1, 1))
      .then(runChild(1, DOMAIN_KEY))                                   // w2 = C key "k"
      .then(claim(1, 2))
      .then(complete(1))                                               // w2 succeeded, w1 woken
      .then(claim(1, 1))
      .then(runChild(1, nextAutoKey(1)))                               // w3 = C auto key, root w1
      .then(claim(1, 3))
      .then(runAct(1))                                                 // a1 for w3
      .then(tw1Claim(1, 1))
      .then(handler(1, 1, "permanent"))
      .then(finish(1, 1, "permanent"))
      .expect(db.wf.get(3).status == "blocked" and db.wf.get(2).status == "succeeded")
      .then(tx2("C", true))
      .expect(not(inv_N1_tx2OwnLineage) and db.wf.get(3).status == "cancelled"
              and db.wf.get(4).restartedFrom == 3 and not(inv_S24_parentWakes))

  // G11: cancelling a parent leaves its child running
  run g11CancelTest =
    init
      .then(start(NO_KEY))
      .then(claim(1, 1))
      .then(runChild(1, nextAutoKey(1)))
      .then(all { keepPrev, TX3_Cancel(1, now) })
      .expect(db.wf.get(2).status == "ready" and not(inv_G11_cancelReachesChildren) and safety)

  // S2/S3: a stale coordinator (lease expired and recovered, row re-claimed) cannot commit
  run staleCoordinatorTest =
    init
      .then(start(NO_KEY))
      .then(claim(1, 1))                                               // token 1
      .then(tick).then(tick).then(tick)
      .then(all { keepPrev, TC1_Claim(2, 1, 1, 2, now + LEASE_W, now) }) // r2 recovers and re-claims w1
      .expect(not(wit_S3_concurrentStep))
      .then(all { keepPrev, CoordFenceMiss(1, 1, 1) })                 // r1's T-C2 loses the fence
      .expect(ghost.taskErrors.get(1) == 1 and safety)
      .then(complete(2))
      .expect(db.wf.get(1).status == "succeeded" and safety)

  // witness: the happy path reaches activity success and wakes the workflow (replay form of T-W1)
  run activitySuccessTest =
    init
      .then(startAndRunActivity(1))
      .then(tw1Replay(2, 1, 0))
      .then(handler(2, 1, "succeeded"))
      .then(finish(2, 1, "succeeded"))
      .expect(not(wit_activitySucceeded) and db.wf.get(1).status == "ready" and safetyRc)

  // T-X1 (P3a semantics): dedup hit returns the row; restart-key collision -> Conflict, no row
  run startSemanticsTest =
    init
      .then(all { keepPrev, TX1_Start(1, "P", DOMAIN_KEY, 0, true, now) })
      .then(all { keepPrev, TX1_Start(1, "P", DOMAIN_KEY, 0, false, now) })
      .expect(lastAction == "TX1_StartExisting" and db.nextWf == 2)
      .then(claim(1, 1))
      .then(complete(1))                                               // w1 succeeded
      .then(all { keepPrev, TX1_Start(2, "P", NO_KEY, 1, true, now) }) // successor of w1
      .expect(lastAction == "TX1_Start" and db.wf.get(2).restartedFrom == 1)
      .then(all { keepPrev, TX1_Start(0, "P", NO_KEY, 1, false, now) })
      .expect(lastAction == "TX1_StartConflict" and db.nextWf == 3 and safety and inv_S19_sourceTerminal)

  // both a dedup key and a restart source: rejected before any SQL (no step)
  run startBothKeysRejectedTest =
    init
      .then(start(NO_KEY))
      .then(all { keepPrev, TX1_Start(2, "P", DOMAIN_KEY, 1, true, now) })
      .fail()

  // N3: the public restart field accepts a live source
  run n3LiveSourceTest =
    init
      .then(start(NO_KEY))
      .then(all { keepPrev, TX1_Start(2, "P", NO_KEY, 1, true, now) })
      .expect(db.wf.get(1).status == "ready" and db.wf.get(2).restartedFrom == 1
              and not(inv_S19_sourceTerminal) and safety)

  // T-X2 restart-key collision: an application start already took w1's restart key, so the
  // recovery's successor insert conflicts and the savepoint rolls back the whole T-X2
  run tx2ConflictTest =
    init
      .then(tx2("P", false))                                           // no original: w1 (P, "k")
      .expect(lastAction == "TX2_StartNew")
      .then(claim(1, 1))
      .then(all { keepPrev, TC3_ActivationFailure(1, 1, ct(1), 1, MAX_ACTIVATION, now, now) })
      .then(claim(1, 1))
      .then(all { keepPrev, TC3_ActivationFailure(1, 1, ct(1), 2, MAX_ACTIVATION, now, now) })  // w1 failed
      .expect(db.wf.get(1).status == "failed")
      .then(all { keepPrev, TX1_Start(2, "P", NO_KEY, 1, true, now) }) // w2 restartedFrom w1, root 0
      .then(tx2("P", false))
      .expect(lastAction == "TX2_Conflict" and db.nextWf == 3 and db.wf.get(1).status == "failed" and safety)

  //---------------------------------------------------------------- operator actions
  action pause(w: int): bool = all { keepPrev, AdminPause(w, now) }
  action resume(w: int): bool = all { keepPrev, AdminResume(w, now) }
  // an activity with maxAttempts 1 (room for the pause bump within MAX_ATTEMPTS = 2)
  action runAct1(r: int): bool = all { keepPrev, TC2_RunActivity(r, cw(r), ct(r), db.nextAct, "t", 1, false, false, now, now) }
  action startAndRunActivity1(r: int): bool = start(NO_KEY).then(claim(r, db.nextWf - 1)).then(runAct1(r))

  // T-A2/T-A3: pausing a running row revokes the claim; the coordinator's commit misses the
  // fence (G1: operator-caused task error); resume derives ready from the empty wait
  run pauseResumeTest =
    init
      .then(start(NO_KEY))
      .then(claim(1, 1))
      .then(pause(1))
      .expect(lastAction == "AdminPause" and db.wf.get(1).status == "paused" and db.wf.get(1).token == 0 and safety)
      .then(all { keepPrev, CoordFenceMiss(1, 1, 1) })
      .expect(ghost.opTaskErrors.get(1) == 1 and not(inv_G1_noSelfCancelFromOperator) and safety)
      .then(tick)
      .then(resume(1))
      .expect(lastAction == "AdminResume" and db.wf.get(1).status == "ready" and db.wf.get(1).availableAt == 1 and safety)
  run pauseTwiceRejectedTest = init.then(start(NO_KEY)).then(pause(1)).then(pause(1)).fail()
  run resumeNotPausedRejectedTest = init.then(start(NO_KEY)).then(resume(1)).fail()

  // G1: with MAX_TASK_RESTARTS = 1, two operator pauses during claims stop runtime 1
  action g1TwoPauses: bool =
    init
      .then(start(NO_KEY)).then(start(NO_KEY)).then(start(NO_KEY))
      .then(claim(1, 1)).then(pause(1)).then(all { keepPrev, CoordFenceMiss(1, 1, 1) })
      .then(claim(1, 2)).then(pause(2)).then(all { keepPrev, CoordFenceMiss(1, 2, 2) })
  run g1SelfCancelTest =
    g1TwoPauses.expect(not(wit_selfCancelled) and ghost.opTaskErrors.get(1) == 2 and not(inv_G1_noSelfCancelFromOperator) and safety)
  run g1StoppedRuntimeClaimsNothingTest = g1TwoPauses.then(claim(1, 3)).fail()
  // an application cancel (T-X3) is not operator-caused
  run g1AppCancelNotOperatorTest =
    init.then(start(NO_KEY)).then(claim(1, 1)).then(all { keepPrev, TX3_Cancel(1, now) })
      .then(all { keepPrev, CoordFenceMiss(1, 1, 1) })
      .expect(ghost.taskErrors.get(1) == 1 and inv_G1_noSelfCancelFromOperator)

  // S36 + N2: pausing a workflow whose activity runs sets the activity pending with
  // maxAttempts + 1 and closes the attempt; the paused handler still executes while another
  // claim takes the cap-1 slot
  run n2PauseFreesSlotTest =
    init
      .then(startAndRunActivity1(1))
      .then(startAndRunActivity1(1))
      .then(tw1Claim(1, 1))                                            // r1 runs a1 (token 3), cap 1 reached
      .then(pause(1))
      .expect(lastAction == "AdminPauseActivity" and db.act.get(1).status == "pending" and db.act.get(1).maxAttempts == 2
              and db.act.get(1).token == 0 and not(db.att.get(1).get(1).open) and not(wit_pausedActivity) and safety)
      .then(tw1Claim(2, 2))                                            // r2 claims a2
      .expect(proc.execs.size() == 2 and not(inv_S13_topicConcurrency) and safety and inv_S17_capAlways)
      .then(all { keepPrev, HandlerReturn(1, 1, 3, "succeeded") })       // the paused handler's lease token
      .then(all { keepPrev, TW3_FenceMiss(1, 1, 3) })                  // paused handler's finish misses the fence
      .then(resume(1))
      .expect(db.wf.get(1).status == "waiting_activity" and db.act.get(1).status == "pending" and safety)
  // a paused workflow's pending activity is not claimable
  run pausedActivityNotClaimedTest =
    init.then(startAndRunActivity1(1)).then(pause(1)).then(tw1Replay(1, 1, 0)).fail()

  // a paused parent still gets its child's outcome (stays paused, wait cleared); resume -> ready
  run pausedParentWokenTest =
    init
      .then(start(NO_KEY))
      .then(claim(1, 1))
      .then(runChild(1, nextAutoKey(1)))                               // w2
      .then(pause(1))
      .expect(db.wf.get(1).status == "paused" and db.wf.get(1).waitKind == "child")
      .then(claim(1, 2))
      .then(complete(1))
      .expect(db.wf.get(1).status == "paused" and db.wf.get(1).waitKind == ""
              and deliverable(db, 1).exists(e => e.typ == "child_succeeded" and e.ref == 2) and safety)
      .then(resume(1))
      .expect(db.wf.get(1).status == "ready" and safety and inv_S24_parentWakes)
  // paused while the child runs, resumed before it ends -> waiting_child
  run pausedParentResumesWaitingTest =
    init
      .then(start(NO_KEY)).then(claim(1, 1)).then(runChild(1, nextAutoKey(1)))
      .then(pause(1)).then(resume(1))
      .expect(db.wf.get(1).status == "waiting_child" and db.wf.get(1).waitRef == 2 and safety)

  // G11 via the operator: admin cancel of the parent leaves its child live
  run g11AdminCancelTest =
    init
      .then(start(NO_KEY))
      .then(claim(1, 1))
      .then(runChild(1, nextAutoKey(1)))
      .then(all { keepPrev, AdminCancel(1, now) })
      .expect(lastAction == "AdminCancel" and db.wf.get(2).status == "ready" and not(inv_G11_cancelReachesChildren) and safety)
  // admin cancel of a paused workflow with a pending activity cancels both
  run adminCancelPausedTest =
    init.then(startAndRunActivity1(1)).then(pause(1)).then(all { keepPrev, AdminCancel(1, now) })
      .expect(db.wf.get(1).status == "cancelled" and db.act.get(1).status == "cancelled" and safety)

  // continuation priority: the activity is ready at CONTINUATION_READY_AT (0), before tnow
  run continuationPriorityTest =
    init
      .then(tick)
      .then(start(NO_KEY))
      .then(claim(1, 1))
      .then(all { keepPrev, TC2_RunActivity(1, 1, ct(1), 1, "t", 1, false, true, CONTINUATION_READY_AT, now) })
      .expect(db.act.get(1).availableAt == 0 and safety)
      .then(tw1Replay(1, 1, 0))
      .expect(db.act.get(1).status == "running" and safety)
  run continuationPriorityNowRejectedTest =
    init.then(tick).then(start(NO_KEY)).then(claim(1, 1))
      .then(all { keepPrev, TC2_RunActivity(1, 1, ct(1), 1, "t", 1, false, true, now, now) }).fail()
  run noPriorityEarlyRejectedTest =
    init.then(tick).then(start(NO_KEY)).then(claim(1, 1))
      .then(all { keepPrev, TC2_RunActivity(1, 1, ct(1), 1, "t", 1, false, false, 0, now) }).fail()

  // N1 (ReturnLatest): the keyed child succeeded, a live auto-keyed sibling is newer; T-X2(C, "k")
  // returns the sibling
  run n1ReturnLatestTest =
    init
      .then(start(NO_KEY))
      .then(claim(1, 1))
      .then(runChild(1, DOMAIN_KEY))                                   // w2 = C key "k"
      .then(claim(1, 2))
      .then(complete(1))
      .then(claim(1, 1))
      .then(runChild(1, nextAutoKey(1)))                               // w3 = C auto key, ready
      .then(tx2("C", false))
      .expect(lastAction == "TX2_ReturnLatest" and not(inv_N1_tx2OwnLineage) and safety)
  // ReturnLatest of the keyed row itself stays inside the lineage
  run n1ReturnOwnRowTest =
    init.then(tx2("P", false)).then(tx2("P", false))
      .expect(lastAction == "TX2_ReturnLatest" and inv_N1_tx2OwnLineage)
}

// Historical: T-W1 plain reads use the InnoDB REPEATABLE READ snapshot (code before P4)
module durable_tests_rr {
  import durable(
    RUNTIMES = Set(1, 2), MAX_WF = 4, MAX_ACT = 3, TOPICS = Set("t"), TOPIC_CAP = Map("t" -> 1),
    LOCAL_SLOTS = 1, MAX_ATTEMPTS = 2, MAX_ACTIVATION = 2, LEASE_W = 3, LEASE_A = 3, MAX_TIME = 10,
    DRIFT = 0, RR_SNAPSHOT = true, ENABLE_CHILDREN = true, ENABLE_CRASH = true, ENABLE_ENV_EDITS = false, MAX_TASK_RESTARTS = 1
  ).* from "./durable"

  action tick: bool = all { keepPrev, Tick }
  action startAndRunActivity(r: int): bool =
    all { keepPrev, TX1_Start(db.nextWf, "P", NO_KEY, 0, true, now) }
      .then(all { keepPrev, TC1_Claim(r, 0, db.nextWf - 1, db.nextToken, now + LEASE_W, now) })
      .then(all { keepPrev, TC2_RunActivity(r, proc.claims.get(r).wf, proc.claims.get(r).token, db.nextAct, "t", MAX_ATTEMPTS, false, false, now, now) })
  action tw1Claim(r: int, a: int): bool =
    all { keepPrev, TW1_BeginBatch(r) }
      .then(all { keepPrev, TW1_ReconcileScan(r, "t") })
      .then(all { keepPrev, TW1_Count(r) })
      .then(all { keepPrev, TW1_ClaimRow(r, a, db.nextToken) })
      .then(all { keepPrev, TW1_Commit(r) })
  action prefix: bool =
    init
      .then(startAndRunActivity(1))
      .then(startAndRunActivity(1))
      .then(tw1Claim(1, 1))                                            // a1 token 3
      .then(tick)
      .then(all { keepPrev, TW2_Send(1, 1, 3, 1) })
      .then(tick).then(tick)
      .then(all { keepPrev, LocalDeadline(1, 1, 3) })

  // G7 (pre-P4): heartbeat committed after the snapshot revives a1; in_flight reads the
  // snapshot and misses it; r2 claims a2 -> 2 live leases on a cap-1 topic
  run g7CapExceededTest =
    prefix
      .then(all { keepPrev, TW1_BeginBatch(2) })
      .then(all { keepPrev, TW1_ReconcileScan(2, "t") })               // snapshot: a1 expired
      .then(all { keepPrev, TW2_Commit(1, 1, 4) })                     // leaseExp = 4
      .then(all { keepPrev, TW1_ReconcileRow(2, 1, 4) })               // current read: live, skip
      .then(all { keepPrev, TW1_Count(2) })                            // snapshot: in_flight = 0
      .then(all { keepPrev, TW1_ClaimRow(2, 2, 4) })
      .then(all { keepPrev, TW1_Commit(2) })
      .expect(not(inv_S17_capAlways) and not(inv_S17_capAtClaim) and inv_S13_topicConcurrency and safety)

  // Same schedule with the heartbeat before the snapshot: no violation
  run g7NeedsSnapshotTest =
    prefix
      .then(all { keepPrev, TW2_Commit(1, 1, 4) })
      .then(all { keepPrev, TW1_BeginBatch(2) })
      .then(all { keepPrev, TW1_ReconcileScan(2, "t") })
      .then(all { keepPrev, TW1_Count(2) })
      .then(all { keepPrev, TW1_Commit(2) })
      .expect(inv_S17_capAlways and inv_S17_capAtClaim and proc.execs.size() == 0 and safety)
}

// S13 needs the clock assumption: the process clock lags DB time by DRIFT = 2
module durable_tests_drift {
  import durable(
    RUNTIMES = Set(1, 2), MAX_WF = 4, MAX_ACT = 3, TOPICS = Set("t"), TOPIC_CAP = Map("t" -> 1),
    LOCAL_SLOTS = 1, MAX_ATTEMPTS = 2, MAX_ACTIVATION = 2, LEASE_W = 3, LEASE_A = 3, MAX_TIME = 10,
    DRIFT = 2, RR_SNAPSHOT = false, ENABLE_CHILDREN = true, ENABLE_CRASH = true, ENABLE_ENV_EDITS = false, MAX_TASK_RESTARTS = 1
  ).* from "./durable"

  action tick: bool = all { keepPrev, Tick }
  // r1 claims a1 at t=0 (DB lease to 3, local deadline 4); at t=3 r2 reconciles and re-claims a1
  // (retry delay 0: a RetryPolicy with 100% jitter can give 0 s)
  run driftTwoHandlersTest =
    init
      .then(all { keepPrev, TX1_Start(1, "P", NO_KEY, 0, true, 0) })
      .then(all { keepPrev, TC1_Claim(1, 0, 1, 1, 3, 0) })
      .then(all { keepPrev, TC2_RunActivity(1, 1, 1, 1, "t", MAX_ATTEMPTS, false, false, 0, 0) })
      .then(all { keepPrev, TW1_Claim(1, 0, Map("t" -> 1), [], Map("t" -> 0), [{ a: 1, tok: 2, leaseExp: 3 }]) })
      .then(tick).then(tick).then(tick)                                // t=3: DB lease expired, local deadline 4
      .then(all { keepPrev, TW1_Claim(2, 3, Map("t" -> 1), [{ a: 1, exhausted: false, availableAt: 3 }], Map("t" -> 0), [{ a: 1, tok: 3, leaseExp: 6 }]) })
      .expect(not(inv_S13_oneHandler) and inv_S17_capAtClaim)
}

// G10: an external write leaves a1 with invalid timeout/lease bounds; one claim_batch over
// both topics claims a2 on "u", then hits a1 on "t" and rolls back everything
module durable_tests_env {
  import durable(
    RUNTIMES = Set(1, 2), MAX_WF = 2, MAX_ACT = 2, TOPICS = Set("t", "u"), TOPIC_CAP = Map("t" -> 1, "u" -> 1),
    LOCAL_SLOTS = 2, MAX_ATTEMPTS = 2, MAX_ACTIVATION = 2, LEASE_W = 3, LEASE_A = 3, MAX_TIME = 10,
    DRIFT = 0, RR_SNAPSHOT = false, ENABLE_CHILDREN = false, ENABLE_CRASH = false, ENABLE_ENV_EDITS = true, MAX_TASK_RESTARTS = 1
  ).* from "./durable"

  action startAndRunActivity(r: int, topic: str): bool =
    all { keepPrev, TX1_Start(db.nextWf, "P", NO_KEY, 0, true, now) }
      .then(all { keepPrev, TC1_Claim(r, 0, db.nextWf - 1, db.nextToken, now + LEASE_W, now) })
      .then(all { keepPrev, TC2_RunActivity(r, proc.claims.get(r).wf, proc.claims.get(r).token, db.nextAct, topic,
                                            MAX_ATTEMPTS, false, false, now, now) })
  action prefix: bool =
    init
      .then(startAndRunActivity(1, "t"))                               // a1 on t
      .then(startAndRunActivity(1, "u"))                               // a2 on u
      .then(all { keepPrev, EnvCorruptActivityBounds(1) })
      .expect(db.act.get(1).invalidBounds and inv_G10_noClaimAbort and safety)
      .then(all { keepPrev, TW1_BeginBatch(2) })
      .then(all { keepPrev, TW1_ReconcileScan(2, "u") })
      .then(all { keepPrev, TW1_Count(2) })
      .then(all { keepPrev, TW1_ClaimRow(2, 2, db.nextToken) })        // a2 claimed (uncommitted)
      .then(all { keepPrev, TW1_ReconcileScan(2, "t") })
      .then(all { keepPrev, TW1_Count(2) })
      .expect(proc.tw1.get(2).cands == Set(1))

  run g10InvalidBoundsTest =
    prefix
      .then(all { keepPrev, TW1_Error(2, 1, "invalid_bounds") })
      .expect(and {
        lastAction == "TW1_Error", not(inv_G10_noClaimAbort), safety, inv_S17_capAtClaim,
        db.act.get(2).status == "pending", proc.execs == Set(), proc.topicHolder == 0,   // a2's claim rolled back
        ghost.taskErrors.get(2) == 1,
      })

  // the invalid row is never claimed, and the attempt-cap reason does not match it
  run g10InvalidRowNotClaimedTest = prefix.then(all { keepPrev, TW1_ClaimRow(2, 1, db.nextToken) }).fail()
  run g10WrongReasonTest = prefix.then(all { keepPrev, TW1_Error(2, 1, "attempt_cap") }).fail()

  // replay form: no T-W1 in flight, the recorded error names a1
  run g10ReplayTest =
    init
      .then(startAndRunActivity(1, "t"))
      .then(all { keepPrev, EnvCorruptActivityBounds(1) })
      .then(all { keepPrev, TW1_Error(2, 1, "invalid_bounds") })
      .expect(not(inv_G10_noClaimAbort) and safety and db.act.get(1).status == "pending")
}
