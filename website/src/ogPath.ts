export function getOgImageSlug(contentId: string): string {
  const segments = contentId.split("/").filter(Boolean);

  if (segments.at(-1) === "index") {
    segments.pop();
  }

  return segments.join("/") || "index";
}
