import satori from 'satori';
import sharp from 'sharp';
import { readFileSync } from 'fs';

const FONT_400 = readFileSync('./node_modules/@fontsource/roboto/files/roboto-latin-400-normal.woff');
const FONT_700 = readFileSync('./node_modules/@fontsource/roboto/files/roboto-latin-700-normal.woff');

const CATEGORY_COLORS = {
  commands: '#58a6ff',
  'getting-started': '#3fb950',
  guides: '#3fb950',
  reference: '#d2a8ff',
  advanced: '#ffa657',
  contributing: '#f778ba',
  tools: '#79c0ff',
  roadmap: '#a371f7',
};

function getCategory(slug) {
  const parts = slug.split('/');
  if (parts[0] === 'commands' || parts[0] === 'getting-started' || 
      parts[0] === 'guides' || parts[0] === 'reference' || 
      parts[0] === 'advanced' || parts[0] === 'contributing' || 
      parts[0] === 'tools' || parts[0] === 'roadmap') {
    return parts[0];
  }
  return 'reference';
}

function getCategoryName(category) {
  const names = {
    commands: 'Command',
    'getting-started': 'Getting Started',
    guides: 'Guide',
    reference: 'Reference',
    advanced: 'Advanced',
    contributing: 'Contributing',
    tools: 'Tool',
    roadmap: 'Roadmap',
  };
  return names[category] || 'Documentation';
}

export async function generateOgSvg({ title, description, slug }) {
  const category = getCategory(slug);
  const color = CATEGORY_COLORS[category] || CATEGORY_COLORS.reference;
  const categoryLabel = getCategoryName(category);

  const element = {
    type: 'div',
    props: {
      style: {
        display: 'flex',
        flexDirection: 'row',
        justifyContent: 'flex-start',
        alignItems: 'center',
        width: '100%',
        height: '100%',
        backgroundColor: '#0a0a0a',
        paddingLeft: '60px',
        paddingRight: '60px',
        paddingTop: '40px',
        paddingBottom: '40px',
        position: 'relative',
        overflow: 'hidden',
        fontFamily: 'Roboto',
      },
      children: [
        // Left accent strip
        {
          type: 'div',
          props: {
            style: {
              position: 'absolute',
              left: 0,
              top: 0,
              bottom: 0,
              width: '6px',
              background: color,
            },
          },
        },
        // Logo section
        {
          type: 'div',
          props: {
            style: {
              display: 'flex',
              flexDirection: 'column',
              justifyContent: 'center',
              alignItems: 'center',
              width: '140px',
              flexShrink: 0,
              marginRight: '40px',
              paddingRight: '40px',
              borderRight: '1px solid rgba(255,255,255,0.08)',
            },
            children: [
              {
                type: 'div',
                props: {
                  style: {
                    fontFamily: 'Roboto',
                    fontSize: '56px',
                    fontWeight: 700,
                    color: '#58a6ff',
                    lineHeight: 1,
                  },
                  children: ';',
                },
              },
              {
                type: 'div',
                props: {
                  style: {
                    fontFamily: 'Roboto',
                    fontSize: '14px',
                    fontWeight: 600,
                    color: '#6e7681',
                    letterSpacing: '1px',
                    textTransform: 'uppercase',
                    marginTop: '12px',
                    textAlign: 'center',
                  },
                  children: 'sql-splitter',
                },
              },
            ],
          },
        },
        // Main content
        {
          type: 'div',
          props: {
            style: {
              display: 'flex',
              flexDirection: 'column',
              justifyContent: 'center',
              width: '900px',
              flexShrink: 0,
            },
            children: [
              // Category badge
              {
                type: 'div',
                props: {
                  style: {
                    fontFamily: 'Roboto',
                    fontSize: '12px',
                    fontWeight: 700,
                    color: color,
                    textTransform: 'uppercase',
                    letterSpacing: '2px',
                    marginBottom: '16px',
                  },
                  children: categoryLabel,
                },
              },
              // Title
              {
                type: 'div',
                props: {
                  style: {
                    fontFamily: 'Roboto',
                    fontSize: '56px',
                    fontWeight: 700,
                    letterSpacing: '-1.5px',
                    lineHeight: 1.1,
                    color: '#e6edf3',
                    marginBottom: '16px',
                    whiteSpace: 'pre-wrap',
                  },
                  children: title,
                },
              },
              // Description
              {
                type: 'div',
                props: {
                  style: {
                    fontFamily: 'Roboto',
                    fontSize: '22px',
                    fontWeight: 400,
                    color: '#8b949e',
                    lineHeight: 1.5,
                  },
                  children: description,
                },
              },
            ],
          },
        },
        // Decorative circle
        {
          type: 'div',
          props: {
            style: {
              position: 'absolute',
              width: '400px',
              height: '400px',
              border: '1px solid rgba(255,255,255,0.03)',
              borderRadius: '50%',
              bottom: '-150px',
              right: '-100px',
              pointerEvents: 'none',
            },
          },
        },
      ],
    },
  };

  const svg = await satori(element, {
    width: 1200,
    height: 630,
    fonts: [
      { name: 'Roboto', data: FONT_400, weight: 400, style: 'normal' },
      { name: 'Roboto', data: FONT_700, weight: 700, style: 'normal' },
    ],
  });

  return svg;
}

export async function svgToPng(svg) {
  const svgBuffer = Buffer.from(typeof svg === 'string' ? svg : String(svg));
  return await sharp(svgBuffer).png({ compressionLevel: 9 }).toBuffer();
}
