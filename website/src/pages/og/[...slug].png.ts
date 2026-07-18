import type { APIRoute } from "astro";
import { getCollection } from "astro:content";
import { getOgImageSlug } from "../../ogPath";
import { generateOgSvg, svgToPng } from "./satori-lib.mjs";

interface OgImageProps {
  title: string;
  description: string;
  slug: string;
}

export async function getStaticPaths() {
  const docs = await getCollection("docs");

  return docs.map((doc) => {
    const slug = getOgImageSlug(doc.id);

    return {
      params: { slug },
      props: {
        title: doc.data.title,
        description: doc.data.description || "sql-splitter documentation",
        slug,
      },
    };
  });
}

export const GET: APIRoute<OgImageProps> = async ({ props }) => {
  const { title, description, slug } = props;

  const svg = await generateOgSvg({ title, description, slug });
  const png = await svgToPng(svg);

  return new Response(png, {
    headers: {
      "Content-Type": "image/png",
      "Cache-Control": "public, max-age=31536000, immutable",
    },
  });
};
