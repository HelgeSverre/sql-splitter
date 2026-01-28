import { defineConfig } from "astro/config";
import starlight from "@astrojs/starlight";
import sitemap from "@astrojs/sitemap";
import react from "@astrojs/react";
import indexnow from "astro-indexnow";
import tailwindcss from "@tailwindcss/vite";
import starlightLinksValidator from "starlight-links-validator";
import starlightGitHubAlerts from "starlight-github-alerts";
import { readFileSync, existsSync } from "node:fs";
import { resolve } from "node:path";

// Read version from Cargo.toml or llms.txt at config time
function getVersion() {
  const cargoPath = resolve(import.meta.dirname, "../Cargo.toml");
  if (existsSync(cargoPath)) {
    const content = readFileSync(cargoPath, "utf-8");
    const match = content.match(/^version\s*=\s*"([^"]+)"/m);
    if (match) return match[1];
  }

  const llmsPath = resolve(import.meta.dirname, "llms.txt");
  if (existsSync(llmsPath)) {
    const content = readFileSync(llmsPath, "utf-8");
    const match = content.match(/# sql-splitter (\d+\.\d+\.\d+)/);
    if (match) return match[1];
  }

  return "0.0.0";
}

const SQL_SPLITTER_VERSION = getVersion();

export default defineConfig({
  site: "https://sql-splitter.dev",
  integrations: [
    react(),
    starlight({
      title: "sql-splitter",
      lastUpdated: true,

      pagination: true,
      tableOfContents: { minHeadingLevel: 2, maxHeadingLevel: 3 },
      plugins: [
        starlightLinksValidator({
          exclude: ["/schemas/", "/schemas/**"],
        }),
        starlightGitHubAlerts(),
      ],
      logo: {
        src: "./src/assets/logo.svg",
      },
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
          autogenerate: { directory: "getting-started" },
        },
        {
          label: "Commands",
          autogenerate: { directory: "commands" },
        },
        {
          label: "Cookbook",
          collapsed: true,
          autogenerate: { directory: "cookbook" },
        },
        {
          label: "Reference",
          collapsed: true,
          autogenerate: { directory: "reference" },
        },
        {
          label: "Advanced",
          collapsed: true,
          autogenerate: { directory: "advanced" },
        },
        {
          label: "Contributing",
          collapsed: true,
          autogenerate: { directory: "contributing" },
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
    define: {
      __SQL_SPLITTER_VERSION__: JSON.stringify(SQL_SPLITTER_VERSION),
    },
  },
});
