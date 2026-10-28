# Slice 6b: Durable and jobs on the store (spec)

Status: **draft, open points marked "Decision needed"** (2026-10-27), written by the redesign session after reading
Durable's `Storage` interface (`packages/durable/src/types.ts`), its SQLite storage, schema and migrations, the storage
conformance suite and benchmark, `session/session.ts`, `session/transaction.ts`, `session/forks.ts`, and Chord v2's
store engine, applier, schema, commit step and jobs, in full. Durable's memory, JSONL and IndexedDB storages were not
read (they stay as they are, S15).

The single source for building 6b, together with: `design.md` read in full; the decision log
(`packages/chord/docs/next/decisions.md`, later entries win), in particular "The store", S1, S2, S4, S10, S11, S13, S15,
W1, W12, W13, "Slice 6 first decisions", "Durable observers", "Durable's synchronous observers", "Text is plain UTF-8",
"Failures are watch calls", the "One vocabulary" entries, "The performance and memory bar"; `slice-6a.md` (what 6a
built); the feature map and the carry-over of `slices.md` (slice 6b); the root `AGENTS.md` "Chord v2 Code Style".

6b puts Durable on the store (`StoreStorage`), follows Durable's views as Chord sources (`durableView`), persists jobs,
and extends the applier to every write kind. After 6b a worker has one store: Chord's objects and Durable's Session
commit through one queue, and a commit of either is one seq in one outbox.

## What an app can do after this slice

```ts
// the start code of a session's worker (p3 base code)
const store = await openStore(sqliteBackend(db), ctx);
const worker = createHost<P>({ address: Session({ id }), principal, store, onError });

// the core facet: the harness object opens Durable on the same store
host.serve(Harness, (_, self) => {
  const durable = self.open(async (context) =>
    Durable.open(storeStorage(store, { owner: "session:harness" }), { …, paused: true }, context),
    (h, _reason, context) => h.close(context));
  const view = self.follow(...)             // Decision needed (D9): name and shape
  return { … };
});

// a persisted job: survives a crash
const ImportJob = defineJob("notes.import", {
  version: 1, args: obj({ path: str() }), result: obj({ count: int() }),
  state: ImportState, persist: { resume: "rerun" },
});
const imports = self.job(ImportJob, { run, finish });
await imports.abort(id, ctx);               // stored; Decision needed (D7): the result
```

## 1. `StoreStorage`: Durable's `Storage` on the store

Durable's `Storage` (types.ts, "Atomic persistence boundary for Session records") implemented over a `ChordStore`, so
Durable's Session runs unchanged on it and passes Durable's storage conformance suite
(`createStorageConformance`), across reopen too.

**Where it lives (D1, owner, 2026-10-27: "they are only used if durable is used"):** `StoreStorage` and
`durableView` belong to Durable: `packages/durable/src/next/`, private-only like `packages/chord/src/next`, importing
`@earendil-works/chord/next` (Chord's `./next` package export, private-only, returns for it). Durable already depends
on Chord and Chord imports nothing of Durable (S13); Durable's conformance suite runs in Durable's tests; the folder
goes upstream with Chord v2.

### 1.1 Records

Durable's four record tables (conversations, entries, tasks, submissions) and its ID namespace.

> **Decision needed (D2): how Durable's records are stored.**
> (a) **Declared tables** in the store, written as ordinary `put` writes: `defineTable`s that `StoreStorage` declares
> for itself (SQL names `"<owner>:<table>"`, e.g. `"session:harness:entries"`), each row the record's indexed columns
> (`id`, `conversation_id`, `kind`, `status`, …) plus the whole record in a `json(...)` column. The outbox, export,
> import and the applier handle them as they handle every table; Durable's layout changes later through
> `evolveTable` (Durable's own SQLite migrations, such as v2's `sleeping`, become table versions). Durable's
> secondary indexes map to declared indexes; the partial head index becomes an index on `(conversation_id, has_head,
> id)` (index columns are non-`opt`). Indexed strings that must stay lossless (`kind`, `requestId`, document kinds and
> keys) keep Durable's JSON encoding in their columns (decision "Text is plain UTF-8").
> (b) **Internal tables** of `StoreStorage` with write kinds of their own (`durable.record`), which the outbox,
> export, import and the applier would each learn. More code, a second table mechanism.
> Proposal: (a).

- **Writes:** every `StorageWrite` of a table record becomes a `put` of its row (Durable replaces complete records;
  conversations and entries are immutable and written once). `record_ids` is a declared table too (`id`,
  `record_type`), one `put` per new id (decision "Slice 6 first decisions" (3)).
- **Reads:** `conversation`, `entry` (with its `commitSeq`: a column), `findLatestHeadMarker`, `scanEntries` (both
  orders, the fork chain walked as `SqliteStorage` does), `task`, `scanTasks`, `submission`, `scanSubmissions`,
  `submissionByRequest`, `scanConversations`: queries over the declared tables, Durable's cursors (`{ after, order }`,
  `scan.ts`) kept as they are (built from the last id, not from Chord's table cursors).
- **Validation before the step** (D4 below): what `SqliteStorage` checks inside its transaction
  (`checkGlobalIds`, `prepareDocumentActions`, `checkDocumentActions`) runs before `store.commit`, with plain reads:
  only Durable's Session writes these tables and it serializes its commits, so the reads see the state the step will
  write on (the argument of S11).

### 1.2 IDs

> **Decision needed (D3): one id counter or two.** Durable mints ids before a commit (`mintId`), one namespace across
> conversations, entries, tasks, submissions and documents ("greater than every id minted since this storage was
> opened and every id stored, also across reopen", S10); its documents carry their id into the commit. Chord's store
> assigns its documents' ids inside the step from its own counter (`next_document_id`), and the engine's
> `chord_documents.id` is the primary key for both. Options: (a) **one counter per store**: `mintId` takes from the
> store's counter in memory (persisted as a high-water mark with every commit), Chord's step takes from it too; Chord's
> document ids are then no longer monotonic with seq (6a small item 11) and a Durable id minted but never committed
> is skipped; (b) **two counters**: Durable's from `record_ids` (`coalesce(max(id), 1) + 1` at open, decided), Chord's
> as today, and the engine's documents table keyed by `(namespace, id)` (Durable's document ids live in Durable's
> namespace). Proposal: (b): Durable keeps its id contract unchanged and Chord its step-assigned ids; the composite key
> costs nothing measurable and keeps the two adapters apart (S13: the engine is shared, the namespaces are not).

### 1.3 Documents

Durable's documents on the documents engine (S13): address `(namespace "durable", owner, kind, key?)`, owner the
scope as a string (`session`, `conversation:7`, `task:31`), kind and key JSON-encoded as Durable's indexed strings.
6a built `latest`; 6b adds what Durable needs:

- **`rewindable` records** keep every revision and are not deleted at retirement: `retired_at` is set, revisions
  stay (decision "Writes carry their versions" already defined the archive fields).
- **Reads at a point:** `findDocument(address, at)`, `document(id, at)` (the base at or below `at` plus the deltas up
  to it, in one read transaction, S4), `scanDocuments(scope, at, kind?)`, with Durable's half-open lifetime
  (`createdAt <= at < retiredAt`); a historical read of a `latest` (or session or task) record is a
  `StorageRequestError` ("does not retain historical content").
- **`document.copy`** (S11): `StoreStorage` reads the source at its point before the step and enters the step with a
  plain create carrying the base; the conformance rules (source not changed in the copy batch, a matching record) are
  checked there.
- **Stored version and `deltasSinceBase`** as `StoredDocument` reports them; Durable's records carry no fingerprint:
  the engine's fingerprint column takes `""` for Durable records.
- **Export** carries every revision of a `rewindable` record, retired ones included, with its seq (format 1's
  fields); import and the applier take them.

### 1.4 Commits

- `StoreStorage.commit(writes, adopt, ctx)`: validates (1.1), resolves copies (1.3), stages the writes (table `put`s,
  document creates, changes and retirements) and calls `store.commit(staged, { admit: () => true, adopt: (seq) =>
  adopt(seq) }, ctx)`. The store's step runs `adopt` inside `host.batch.commit(...)` when the store has a host, so
  Durable's observers mark Chord sources in the step (W1, "Slice 6 first decisions" (1)); `adopt` never throws
  (Durable's rule).
- **Durable's seq is the store's seq** (gaps allowed: Chord's commits take seqs too).
- **Errors:**

> **Decision needed (D4): what a refused Durable commit does to the store.** The store's rule: any backend error in a
> step is `storage_failed`, the store stops (Durable's own rule for its Session). Durable's conformance suite commits
> invalid batches on purpose ("ID 1 already belongs to conversation", "already has a current incarnation", "version
> transition requires a base", a mismatched copy) and expects the storage to go on. Proposal: those checks run before
> the step (1.1) and reject there, the store untouched; only a backend failure inside the step stops the store. In
> production the Session fails on any commit error either way (`GuardedStorage`), so the difference is only that the
> store, shared with Chord's objects, does not stop for a Durable bug caught by its checks.

- `mintId`, `close(context)` (closes nothing of the store: the app owns it; later calls reject "closed"), reads after
  a store failure reject as Durable's `GuardedStorage` expects.

### 1.5 Statements per commit

A target to measure, as 6a's: an entry append is the `record_ids` put, the entry put, `lastSeq` (plus the outbox); a
document delta is 6a's three statements. `SqliteStorage` does the same work today (`record_ids`, the row, metadata).

## 2. `durableView`: Durable's views as Chord sources

A Chord source over Durable's synchronous observers (`observeConversation`, `observeTaskGraph`, `observeDoc`;
decision "Durable's synchronous observers"): `advance(call, ctx)` records the call; on the store, `adopt` runs inside
the host's batch, so the source is marked in the step and its frame goes out in the flush with the rest of the commit
(one message per link per commit). On a Durable storage that is not the store (tests, other apps), calls arrive outside
any batch: the source opens one batch per Durable commit on the next microtask (carry-over). Durable's failures are
final: `end({ kind: "failed" })` ends the source's watches `failed`; `ended` ends them `ended`. A derive over two
Durable views of one commit sees both changes in one computation.

> **Decision needed (D9): the API.** The carry-over names it `self.follow` (working name): `self.follow(observe)` in
> a factory, `observe` attaching an observer, returning a `Watchable` usable as a state member, a derive source and an
> `on` source, detached when the attempt ends. Options: (a) `self.follow((observer, ctx) =>
> harness.observeConversation(id, observer, ctx, window))`; (b) a plain function `durableView(attach)` returning a
> native the object opens (`self.open`). Proposal: (a), named `self.follow`: the attempt owns the attachment as it
> owns a derive.

## 3. Persisted jobs

Design §15 "Persistence": `persist: { resume: "rerun" | "abandon" }`, only in `local` objects with empty params (P3,
at declaration); a run's record and state document in the owner's store; a recorded outcome resumes only `finish`; an
abort request is persisted; a restart limit; the job fingerprint stored with every record; the start trace carrier.

- **Where records live:**

> **Decision needed (D5).** (a) **Documents of the object:** each run's record is a keyed persisted document of the
> object (an internal definition per job, key the run id: `{ version, fingerprint, args, key?, phase, outcome?,
> abort?, restarts, trace? }`), its state document a second keyed document; both written in the transactions that
> already change them (start, state change, outcome, finish, block, drop), loaded at the object's first activation
> (all unsettled runs: `running` needs every one). (b) A table per job. Proposal: (a): the records are few, change
> in the same transactions as today, and reuse persisted documents (versions, checkpoints, export).

- **Resume at activation:** a run recorded `finishing` resumes `finish` only; a run recorded `running` was cut off by
  a crash: `rerun` invokes `run` again with the stored args and state, `abandon` records `failed { aborted, reason:
  "restart" }` and finishes.
- **Restart limit** (aligned with Durable's `restarts`):

> **Decision needed (D6).** Durable counts how many times in a row a crash cut a task off without the task changing
> its state since: open counts, a close does not, and at `restartLimit` the task ends visibly. Proposal for Chord:
> the same rule: at activation each run recorded `running` gets `restarts + 1` stored; a `ctx.state.change` resets it
> to 0 (progress); at 5 the run ends `failed { aborted, reason: "restart" }` with a `job_failed` report naming the
> limit (no `blocked { restart_limit }`: Durable dropped parking in #11139, and a blocked run would need a retry API);
> a clean close (`host.close`, a reload's native restart) records nothing: those runs end `aborted` (`stop`/`reload`)
> and do not resume. (Supersedes design §15's "then `blocked { restart_limit }`".)

- **Abort** (owner, 2026-10-27: async and stored in 6b):

> **Decision needed (D7): `job.abort(id, ctx)`'s result.** Cases: a running run (its signal fires, the abort is
> stored so a rerun after a crash does not run again but finishes `aborted`); a finishing run (its outcome is decided,
> nothing to do); a blocked run (dropped); an unknown or settled id. Durable's `abortTask` returns `"marked" |
> "terminal"` and throws for an unknown id. Proposal: `Promise<"marked" | "finishing" | "dropped">`, rejecting `gone`
> for an unknown or settled id (as `outcome(id)` does).

- **Deferred state persistence:**

> **Decision needed (D8).** Design §15: "a persisted state document may use the deferred persistence option (stored
> within about a second, flushed before the outcome is recorded)", as Durable's `CommitPersistence` `deferred`.
> Options: (a) per change: `ctx.state.change(fn, ctx, { persistence: { kind: "deferred", withinMs? } })`, as Durable's
> commit option; (b) per definition: `defineState(..., { persistDeferred: 1000 })`. Proposal: (a), Durable's shape: a
> deferred change is adopted and published at once, stored with the next immediate change of the document, before
> the outcome, at close, or after `withinMs` (default 1,000 ms); a crash loses it (acceptable for progress output).

- **Fingerprint:** the job's fingerprint (args, result and state schemas) stored with every record; a record of the
  same version with another fingerprint is `blocked { version }` (slice 7's family; the carry-over's "a job
  fingerprint over args, result and state").
- **Trace carrier:** the start's carrier (design §29) stored with the record; used in slice 9.

## 4. The applier for batch apply

6a's applier applies an archive's writes; 6b extends it to every `StoreWrite` kind (`alter`, `dropIndex`, `update`,
`updateWhere`, `delete`, `deleteWhere` with their recorded counts and versions, `document.change`, `document.retire`),
checked against the store's records (a trust boundary), one code path with import. A test replays a store's outbox
into an empty store and compares the two stores' exports. Applying a batch with a different count than recorded stops
applying (S17).

## 5. Benchmarks and tests

- **The gate** (decisions "The commit-step benchmark", "The performance and memory bar"): Durable's own storage
  benchmark (`test/storage.bench.ts`: the write and read cases of `testing/storage-benchmark.ts`) on `StoreStorage`
  against `SqliteStorage`, and Durable's `test/conversation-view-bench.ts` on both: each hot path within about 5%,
  memory (allocation per op, retained after GC) no worse beyond noise; persisted job starts and state changes against
  in-memory ones.
- **Tests:** Durable's storage conformance suite on `StoreStorage` (memory `:memory:` and across reopen); Durable's
  Session and harness test files that take a storage, run on `StoreStorage`; durableView (one frame per commit, a
  derive over two views, outside the store, failure and end); persisted jobs (rerun, abandon, a recorded outcome
  resumes `finish`, restart counting and reset, the limit, abort stored, deferred state, fingerprints); the applier
  (every kind, the outbox replay, refused counts); export and import with Durable records and `rewindable` history;
  GC (observers detached with their attempt).
- **Examples:** `26-durable-on-the-store.ts`, `27-durable-views.ts`, `28-persisted-jobs.ts`.

## Carry-over taken

From `slices.md` "Slice 6b": persisted jobs (all of it), `StoreStorage` (ids, fork copies, `adopt` in the batch),
`durableView` (the batch on and off the store, `self.follow`), the applier for batch apply; `job.abort` async and
stored (decision of 2026-10-27).

## Carry-over left

Slice 7: a job fingerprint mismatch's `blocked { version }` resolution on reload; runs of jobs the code no longer
declares (with D5's records). Slice 9: the trace carrier's use.
