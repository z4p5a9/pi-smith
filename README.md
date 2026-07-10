# Smith

Smith is an experimental sub-agent extension for the Pi coding agent, built with Effect v4.
Its name references Agent Smith from _The Matrix_.

The extension is private and contains only its project scaffold. Its sub-agent architecture has not been selected or documented yet.

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

## Security

Pi extensions execute with the full permissions of the Pi process. Review the extension and its dependencies before loading it.
