# Further reading

Background for applying deterministic simulation testing (DST) and
property-based testing (PBT) well. Read the relevant source when a design
question goes beyond this skill.

## Deterministic simulation testing

- [FoundationDB: Simulation and Testing](https://apple.github.io/foundationdb/testing.html):
  the system that popularized running a whole distributed database under one
  seeded, single-threaded simulator.
- [Will Wilson, "Testing Distributed Systems w/ Deterministic Simulation"](https://www.youtube.com/watch?v=4fFDFbi3toc)
  (Strange Loop 2014): the FoundationDB talk on why seeds, fault injection and
  replay find bugs that ordinary tests do not.
- [Phil Eaton, "What's the big deal about Deterministic Simulation Testing?"](https://notes.eatonphil.com/2024-08-20-deterministic-simulation-testing.html):
  a practical introduction with code, including how to isolate time, randomness
  and I/O.
- [TigerBeetle: Safety](https://docs.tigerbeetle.com/concepts/safety/) and the
  [VOPR simulator notes](https://github.com/tigerbeetle/tigerbeetle/blob/main/docs/internals/vopr.md):
  a financial database built around a deterministic simulator with fault injection.
- [sled simulation guide](https://sled.rs/simulation.html): designing code so it
  can be driven by a simulator in the first place.
- [Antithesis, "Is something bugging you?"](https://antithesis.com/blog/is_something_bugging_you/):
  the FoundationDB authors on taking DST beyond a single codebase.
- [Jepsen](https://jepsen.io/): black-box consistency testing of real distributed
  systems. Useful for choosing which properties to check.

## Property-based testing

- [Hegel for TypeScript](https://hegel.dev/typescript): the generator library used
  in Cloudflake's examples. Draw workload values and the seed so failures shrink.
- [Hypothesis](https://hypothesis.readthedocs.io/en/latest/): the Python PBT
  library that inspired Hegel, which is developed in part by Hypothesis
  developers. Its docs explain strategies, shrinking and stateful testing well.
- [fast-check](https://fast-check.dev/): a widely used JavaScript PBT library,
  including model-based and race-condition testing.
- [Claessen and Hughes, "QuickCheck"](https://www.cs.tufts.edu/~nr/cs257/archive/john-hughes/quick.pdf)
  (ICFP 2000): the original paper on stating properties and generating inputs.

## Cloudflare semantics worth testing against

- [Queues delivery guarantees](https://developers.cloudflare.com/queues/reference/delivery-guarantees/):
  at-least-once delivery, which is why consumers need idempotency.
- [Durable Object alarms](https://developers.cloudflare.com/durable-objects/api/alarms/):
  one alarm per object, retry behavior and at-least-once execution.
- [Miniflare](https://developers.cloudflare.com/workers/testing/miniflare/): the
  local workerd runtime behind `cloudflake/miniflare` and its options.

## Agent skills

- [Agent Skills format](https://agentskills.io): the `SKILL.md` convention this
  skill follows.
- [skills.sh](https://skills.sh): the `npx skills` installer.
