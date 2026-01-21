import { getCollection } from 'astro:content';
import { generateOgSvg, svgToPng } from './satori-lib.mjs';

export async function getStaticPaths() {
  const docs = await getCollection('docs');

  return docs.map((doc) => {
    // Remove .mdx extension and /index suffix to match routeData.ts URL generation
    const slug = doc.id
      .replace(/\.mdx$/, '')
      .replace(/\/index$/, '') || 'index';

    return {
      params: { slug },
      props: {
        title: doc.data.title,
        description: doc.data.description || 'sql-splitter documentation',
        slug,
      },
    };
  });
}

export async function GET({ props }) {
  const { title, description, slug } = props;
  
  const svg = await generateOgSvg({ title, description, slug });
  const png = svgToPng(svg);
  
  return new Response(png, {
    headers: {
      'Content-Type': 'image/png',
      'Cache-Control': 'public, max-age=31536000, immutable',
    },
  });
}
