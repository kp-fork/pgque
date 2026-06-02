import { defineCollection } from 'astro:content';
import { glob } from 'astro/loaders';
import { docsSchema } from '@astrojs/starlight/schema';

/*
 * Single source of truth: the repo's `docs/*.md` files live one level up, in
 * `../docs`. We read them in place with a glob loader instead of copying them
 * into `src/content/docs`, so GitHub and the website never drift.
 *
 * generateId prefixes every page with `docs/` so the site serves docs under
 * `/docs/...` and leaves `/` for the custom landing page. `README.md` becomes
 * the `/docs` index.
 */
export const collections = {
  docs: defineCollection({
    loader: glob({
      pattern: '**/[^_]*.md',
      base: '../docs',
      generateId: ({ entry }) => {
        const slug = entry.replace(/\.md$/, '');
        return slug === 'README' ? 'docs' : `docs/${slug}`;
      },
    }),
    schema: docsSchema(),
  }),
};
