import type { StarlightRouteData } from "@astrojs/starlight/route-data";
import { defineRouteMiddleware } from "@astrojs/starlight/route-data";
import { getOgImageSlug } from "./ogPath";

export const onRequest = defineRouteMiddleware((context, next) => {
  const routeData = context.locals.starlightRoute as
    StarlightRouteData | undefined;
  if (!routeData) return next();

  const slug = getOgImageSlug(routeData.id);
  const ogImageUrl = new URL(`/og/${slug}.png`, context.site ?? context.url)
    .href;

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
        property: "og:image:width",
        content: "1200",
      },
    },
    {
      tag: "meta",
      attrs: {
        property: "og:image:height",
        content: "630",
      },
    },
    {
      tag: "meta",
      attrs: {
        property: "og:image:type",
        content: "image/png",
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
