# Project instructions

- Read `docs/effect-v4/LLMS.md` before writing or changing Effect code.
- Use the Node version pinned by `mise.toml`.
- After a clean dependency install, run `npm run setup:effect` once.
- Run `npm run check` before declaring work complete.
- Use `npm run fix` only for safe lint and formatting fixes; inspect the resulting diff.
- Never use dangerous automatic fixes without explicit approval.
- Keep Effect v4 and every `@effect/*` package on the same exact beta version.
- Keep runtime imports in `dependencies`; imported Pi packages belong in `peerDependencies: "*"` and exact local `devDependencies`.
- Do not add a build step. Pi loads `src/index.ts` directly through Jiti.
- Do not add abstractions or implementation policy before concrete behavior requires them.
- Keep code linear, explicit, co-located, and direct.
