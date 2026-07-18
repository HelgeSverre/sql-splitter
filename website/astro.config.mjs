import { defineConfig, envField } from "astro/config";
import starlight from "@astrojs/starlight";
import sitemap from "@astrojs/sitemap";
import indexnow from "astro-indexnow";
import tailwindcss from "@tailwindcss/vite";
import starlightLinksValidator from "starlight-links-validator";
import starlightGitHubAlerts from "starlight-github-alerts";
import starlightLlmsTxt from "starlight-llms-txt";
import packageJson from "./package.json" with { type: "json" };

export default defineConfig({
  site: "https://sql-splitter.dev",
  env: {
    schema: {
      SQL_SPLITTER_VERSION: envField.string({
        context: "server",
        access: "public",
        default: packageJson.version,
      }),
    },
  },
  integrations: [
    starlight({
      title: "sql-splitter",
      lastUpdated: true,

      pagination: true,
      tableOfContents: { minHeadingLevel: 2, maxHeadingLevel: 3 },
      plugins: [
        starlightLlmsTxt({
          projectName: "sql-splitter",
          description:
            "A fast Rust CLI and library for inspecting and transforming SQL dumps.",
          promote: ["index*", "getting-started/**", "commands/index"],
        }),
        starlightLinksValidator({
          exclude: ["/schemas/", "/schemas/**"],
        }),
        starlightGitHubAlerts(),
      ],
      customCss: ["./src/styles/starlight-custom.css"],
      social: [
        {
          icon: "github",
          label: "GitHub",
          href: "https://github.com/helgesverre/sql-splitter",
        },
      ],
      sidebar: [
        {
          label: "Getting Started",
          items: [{ autogenerate: { directory: "getting-started" } }],
        },
        {
          label: "Commands",
          items: [{ autogenerate: { directory: "commands" } }],
        },
        {
          label: "Cookbook",
          collapsed: true,
          items: [{ autogenerate: { directory: "cookbook" } }],
        },
        {
          label: "Reference",
          collapsed: true,
          items: [{ autogenerate: { directory: "reference" } }],
        },
        {
          label: "Advanced",
          collapsed: true,
          items: [{ autogenerate: { directory: "advanced" } }],
        },
        {
          label: "Contributing",
          collapsed: true,
          items: [{ autogenerate: { directory: "contributing" } }],
        },
        { label: "Roadmap", link: "/roadmap/" },
      ],
      head: [
        {
          tag: "link",
          attrs: {
            rel: "preconnect",
            href: "https://fonts.googleapis.com",
          },
        },
        {
          tag: "link",
          attrs: {
            rel: "preconnect",
            href: "https://fonts.gstatic.com",
            crossorigin: true,
          },
        },
        {
          tag: "link",
          attrs: {
            href: "https://fonts.googleapis.com/css2?family=Monda:wght@400;700&display=swap",
            rel: "stylesheet",
          },
        },
        {
          tag: "script",
          attrs: {
            src: "https://analytics.ahrefs.com/analytics.js",
            "data-key": "H3wTjxTyPrwBj0sBuePwhQ",
            async: true,
          },
        },
      ],
      components: {
        SiteTitle: "./src/components/SiteTitle.astro",
        ThemeSelect: "./src/components/ThemeToggle.astro",
        SocialIcons: "./src/components/SocialIcons.astro",
      },
      routeMiddleware: "./src/routeData.ts",
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
