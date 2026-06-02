# PgQue website (`web/`)

The marketing landing page and documentation site for [pgque.dev](https://pgque.dev),
built with [Astro](https://astro.build) + [Starlight](https://starlight.astro.build).

- `/` — custom landing page (`src/pages/index.astro`).
- `/docs/*` — the repo's `docs/*.md`, read in place via a glob loader
  (`src/content.config.ts`, base `../docs`). The Markdown in `docs/` is the
  single source of truth; the site adds only sidebar grouping and frontmatter
  titles. Edit the docs in `../docs`, not here.

## Develop

```bash
cd web
npm install
npm run dev        # http://localhost:4321
```

## Build and preview

```bash
npm run build      # static output in web/dist/
npm run preview    # serve the built site locally
```

## Deploy

`web/dist/` is a static bundle. The intended target is `pgque.dev` via a static
host (Cloudflare Pages / Netlify / GitHub Pages). Build command `npm run build`,
output directory `web/dist`, project root `web/`. No CI/CD deploy is wired up yet.
