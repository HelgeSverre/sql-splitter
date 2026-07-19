import { defineConfig, envField } from "astro/config";
import starlight from "@astrojs/starlight";
import sitemap from "@astrojs/sitemap";
import indexnow from "astro-indexnow";
import tailwindcss from "@tailwindcss/vite";
import starlightLinksValidator from "starlight-links-validator";
import starlightGitHubAlerts from "starlight-github-alerts";
import starlightLlmsTxt from "starlight-llms-txt";
import { readFileSync } from "node:fs";
import { resolve } from "node:path";
import remarkExplicitHeadingIds from "./src/remark-explicit-heading-ids.mjs";

function getVersion() {
  const cargoPath = resolve(import.meta.dirname, "../Cargo.toml");
  let cargoContent;

  try {
    cargoContent = readFileSync(cargoPath, "utf8");
  } catch (cause) {
    throw new Error(`Could not read version source at ${cargoPath}`, { cause });
  }

  const versionMatch = cargoContent.match(/^version\s*=\s*"([^"]+)"/m);
  if (!versionMatch) {
    throw new Error(`Could not find package version in ${cargoPath}`);
  }

  return versionMatch[1];
}

const SQL_SPLITTER_VERSION = getVersion();

export default defineConfig({
  site: "https://sql-splitter.dev",
  markdown: {
    remarkPlugins: [remarkExplicitHeadingIds],
  },
  env: {
    schema: {
      SQL_SPLITTER_VERSION: envField.string({
        context: "server",
        access: "public",
        default: SQL_SPLITTER_VERSION,
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
          items: [
            { slug: "commands", label: "Overview" },
            "commands/analyze",
            "commands/completions",
            "commands/convert",
            "commands/diff",
            "commands/generate",
            "commands/graph",
            "commands/merge",
            "commands/order",
            "commands/query",
            "commands/redact",
            "commands/sample",
            "commands/shard",
            "commands/split",
            "commands/validate",
          ],
        },
        {
          label: "Synthetic data",
          items: [
            "commands/generate/model-reference",
            {
              slug: "commands/generate/generators",
              label: "Generator reference",
            },
            "commands/generate/modifiers",
            "commands/generate/planners",
            "commands/generate/inference",
            "commands/generate/privacy-verification",
            {
              slug: "commands/generate/diagnostics",
              label: "Diagnostics",
            },
            {
              slug: "commands/generate/library-api",
              label: "Rust API",
            },
          ],
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
