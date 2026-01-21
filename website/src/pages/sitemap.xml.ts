import type { APIRoute } from "astro";

export const GET: APIRoute = () => {
  // In production, serve the actual sitemap-index.xml content
  // This avoids XML parse errors from HTML redirects
  const siteUrl = "https://sql-splitter.dev";

  const xml = `<?xml version="1.0" encoding="UTF-8"?>
<sitemapindex xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
  <sitemap>
    <loc>${siteUrl}/sitemap-0.xml</loc>
  </sitemap>
</sitemapindex>`;

  return new Response(xml, {
    headers: {
      "Content-Type": "application/xml",
    },
  });
};
