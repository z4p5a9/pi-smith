# Smith

Smith is an experimental sub-agent extension for the Pi coding agent, built with Effect v4.
Its name references Agent Smith from _The Matrix_.

The root agent creates a sub-agent with a title and prompt. Smith launches it in a separate execution environment, supervises its lifetime, communicates with it, and delivers its response back into the root agent's context.

This project is green-filled and currently in alpha so everything is subject to change.

## Current state

Smith currently:

- runs Pi sub-agents in dedicated CMUX panes;
- admits sub-agents through a FIFO queue and executes up to ten concurrently;
- models each sub-agent as a process that owns its full lifecycle and always resolves a
  result: exited, failed, or killed — including on interruption;
- generates stable sub-agent IDs and projects queued, starting, running, completed, and
  failed states into a best-effort in-memory checkpoint that never gates execution;
- communicates over a Unix-socket protocol with per-frame identity and version
  validation, where acknowledgements mean "received" and the first valid frame of any
  kind establishes the session; `seq` and `ack` are non-negative safe-integer
  correlation identifiers, receive order is physical wire order, and they provide no
  replay protection or numeric ordering;
- contains malformed, oversized, or misidentified connections without poisoning the
  listener, the pool, or unrelated sub-agents;
- delivers child messages and failures as hidden root-context messages with UI
  notifications, aggregated into one root event stream;
- runs each child ephemerally: one prompt, one report — then the root tears down the
  Host and the closing pane takes the child with it; children die on transport loss,
  never by deciding their own shutdown;
- releases the host, pane, and worker capacity deterministically when a process ends.

The current implementation supports only the Pi harness and CMUX pane host. It has no
persistent storage, resumption, follow-up messaging, stop controls, workflow scripting,
widget, or complete terminal-state projection.

The domain language and boundary map live in `CONTEXT.md`; the decisions behind them in
`docs/adr/`.

## Intended direction

Smith is intended to become a general sub-agent runtime and orchestration system with:

- multiple hosts, including CMUX, tmux, and microVMs such as Firecracker;
- multiple harnesses, including Pi, Codex, Claude Code, and future agent systems;
- persistent checkpoints, recovery across root restarts, and resumable sub-agents;
- long-lived sub-agents that can become idle and receive additional work;
- send, interrupt, stop, list, focus, and resume operations;
- accurate lifecycle projection and a root UI widget;
- host lifecycle observation, reconciliation, bounded cleanup, and recovery from partial failures;
- a stable process and communication model independent of the selected host or harness;
- strong failure isolation, so malformed messages, failed children, unavailable hosts, and cleanup failures remain contained to the affected operation while healthy workers, listeners, and sub-agents continue operating;
- programmable TypeScript workflows for deterministic, multi-phase sub-agent execution;
- sequential and parallel sub-agent composition;
- schema-defined outputs that are parsed, validated, combined, and passed to downstream sub-agents.

The end state supports both interactive delegation and deterministic, typed, failure-tolerant multi-agent workflows.

## Requirements

- [mise](https://mise.jdx.dev/)
- Pi `0.80.6` for local development
- Node.js `>=22.19.0`; development is pinned to Node `24.18.0` LTS in `mise.toml`
- npm with the committed `package-lock.json`

Effect v4 is currently beta and is pinned exactly to `4.0.0-beta.97`.

## Setup

```bash
mise install
npm ci --ignore-scripts
npm run setup:effect
npm run check
```

Run `npm run setup:effect` once after every clean install or change to TypeScript or `@effect/tsgo`.

## Development

```bash
npm run dev
```

This loads the package root through Pi using `pi -e .`. Run `npm run check` as the authoritative local and CI gate.
