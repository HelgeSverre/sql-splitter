import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import sitemap from '@astrojs/sitemap';
import react from '@astrojs/react';
import indexnow from 'astro-indexnow';
import tailwindcss from '@tailwindcss/vite';
import starlightLinksValidator from 'starlight-links-validator';
import starlightGitHubAlerts from 'starlight-github-alerts';

export default defineConfig({
  site: 'https://sql-splitter.dev',
  integrations: [
    react({ jsxRuntime: 'classic' }),
    starlight({
      title: 'sql-splitter',
      lastUpdated: true,
      editLink: {
        baseUrl: 'https://github.com/helgesverre/sql-splitter/edit/main/website/'
      },
      pagination: true,
      tableOfContents: { minHeadingLevel: 2, maxHeadingLevel: 3 },
      plugins: [
        starlightLinksValidator({
          exclude: ['/schemas/', '/schemas/**'],
        }),
        starlightGitHubAlerts(),
      ],
      logo: {
        src: './src/assets/logo.svg',
      },
      customCss: ['./src/styles/starlight-custom.css'],
      social: [
        { icon: 'github', label: 'GitHub', href: 'https://github.com/helgesverre/sql-splitter' },
      ],
      sidebar: [
        {
          label: 'Getting Started',
          autogenerate: { directory: 'getting-started' },
        },
        {
          label: 'Commands',
          autogenerate: { directory: 'commands' },
        },
        {
          label: 'Guides',
          autogenerate: { directory: 'guides' },
        },
        {
          label: 'Reference',
          autogenerate: { directory: 'reference' },
        },
        {
          label: 'Advanced',
          autogenerate: { directory: 'advanced' },
        },
        {
          label: 'Contributing',
          autogenerate: { directory: 'contributing' },
        },
        { label: 'Roadmap', link: '/roadmap/' },
      ],
      head: [
        {
          tag: 'link',
          attrs: {
            rel: 'preconnect',
            href: 'https://fonts.googleapis.com',
          },
        },
        {
          tag: 'link',
          attrs: {
            rel: 'preconnect',
            href: 'https://fonts.gstatic.com',
            crossorigin: true,
          },
        },
        {
          tag: 'link',
          attrs: {
            href: 'https://fonts.googleapis.com/css2?family=Monda:wght@400;700&display=swap',
            rel: 'stylesheet',
          },
        },
        {
          tag: 'script',
          attrs: {
            src: 'https://analytics.ahrefs.com/analytics.js',
            'data-key': 'H3wTjxTyPrwBj0sBuePwhQ',
            async: true,
          },
        },
      ],
      components: {
        SiteTitle: './src/components/SiteTitle.astro',
        ThemeSelect: './src/components/ThemeToggle.astro',
        SocialIcons: './src/components/SocialIcons.astro',
      },
      routeMiddleware: './src/routeData.ts',
    }),
    sitemap(),
    indexnow({
      key: process.env.INDEXNOW_KEY,
      enabled: !!process.env.INDEXNOW_KEY,
    }),
  ],
  vite: {
    plugins: [tailwindcss()],
  },
});
