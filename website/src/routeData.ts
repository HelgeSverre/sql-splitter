import { defineRouteMiddleware } from "@astrojs/starlight/route-data";
import type { StarlightRouteData } from "@astrojs/starlight/route-data";

export const onRequest = defineRouteMiddleware((context, next) => {
  const routeData = context.locals.starlightRoute as
    | StarlightRouteData
    | undefined;
  if (!routeData) return next();

  // Get the slug from the entry id (e.g., "getting-started/index.mdx" -> "getting-started")
  // astro-og-canvas strips the /index suffix from the path
  const slug = routeData.id
    .replace(/\.(mdx?|md)$/, "") // Remove extension
    .replace(/\/index$/, ""); // Remove /index suffix
  const ogImageUrl = `${context.site}og/${slug || "index"}.png`;

  // Add OG image meta tags
  routeData.head.push(
    {
      tag: "meta",
      attrs: {
        property: "og:image",
        content: ogImageUrl,
      },
    },
    {
      tag: "meta",
      attrs: {
        name: "twitter:image",
        content: ogImageUrl,
      },
    },
    {
      tag: "meta",
      attrs: {
        name: "twitter:card",
        content: "summary_large_image",
      },
    },
  );

  return next();
});
