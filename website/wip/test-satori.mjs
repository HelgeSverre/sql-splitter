import satori from 'satori';
import { Resvg } from '@resvg/resvg-js';
import { readFileSync, writeFileSync } from 'fs';

async function generateOgImage() {
  const fontRegular = readFileSync('/tmp/roboto-400.woff');
  const fontBold = readFileSync('/tmp/roboto-700.woff');

  const svg = await satori(
    {
      type: 'div',
      props: {
        style: {
          display: 'flex',
          flexDirection: 'column',
          justifyContent: 'center',
          alignItems: 'flex-start',
          width: '100%',
          height: '100%',
          backgroundColor: '#0a0a0a',
          padding: '60px',
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
                display: 'flex',
                position: 'absolute',
                left: 0,
                top: 0,
                bottom: 0,
                width: '6px',
                background: 'linear-gradient(180deg, #58a6ff 0%, #3fb950 50%, #ffa657 100%)',
              },
            },
          },
          // Content container
          {
            type: 'div',
            props: {
              style: {
                display: 'flex',
                alignItems: 'center',
                gap: '40px',
              },
              children: [
                // Logo
                {
                  type: 'div',
                  props: {
                    style: {
                      display: 'flex',
                      flexDirection: 'column',
                      alignItems: 'center',
                      gap: '12px',
                      paddingRight: '40px',
                      borderRight: '1px solid rgba(255,255,255,0.08)',
                    },
                    children: [
                      {
                        type: 'div',
                        props: {
                          style: {
                            fontFamily: 'SF Mono, monospace',
                            fontSize: '56px',
                            fontWeight: 700,
                            color: '#58a6ff',
                          },
                          children: ';',
                        },
                      },
                      {
                        type: 'div',
                        props: {
                          style: {
                            fontSize: '14px',
                            fontWeight: 600,
                            color: '#6e7681',
                            letterSpacing: '1px',
                            textTransform: 'uppercase',
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
                      flex: 1,
                      flexDirection: 'column',
                    },
                    children: [
                      // Category
                      {
                        type: 'div',
                        props: {
                          style: {
                            display: 'flex',
                            fontSize: '12px',
                            fontWeight: 600,
                            color: '#58a6ff',
                            textTransform: 'uppercase',
                            letterSpacing: '2px',
                            marginBottom: '16px',
                          },
                          children: 'Command',
                        },
                      },
                      // Title
                      {
                        type: 'div',
                        props: {
                          style: {
                            display: 'flex',
                            fontSize: '52px',
                            fontWeight: 700,
                            letterSpacing: '-1px',
                            lineHeight: 1.15,
                            color: '#e6edf3',
                            marginBottom: '16px',
                          },
                          children: 'split',
                        },
                      },
                      // Description
                      {
                        type: 'div',
                        props: {
                          style: {
                            display: 'flex',
                            fontSize: '20px',
                            color: '#8b949e',
                            lineHeight: 1.5,
                            maxWidth: '600px',
                          },
                          children: 'Split a SQL dump file into individual table files at 600+ MB/s',
                        },
                      },
                    ],
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
                display: 'flex',
                position: 'absolute',
                width: '300px',
                height: '300px',
                border: '1px solid rgba(88, 166, 255, 0.08)',
                borderRadius: '50%',
                bottom: '-100px',
                right: '-50px',
              },
            },
          },
        ],
      },
    },
    {
      width: 1200,
      height: 630,
      fonts: [
        {
          name: 'Roboto',
          data: fontRegular,
          weight: 400,
          style: 'normal',
        },
        {
          name: 'Roboto',
          data: fontBold,
          weight: 700,
          style: 'normal',
        },
      ],
    }
  );

  const resvg = new Resvg(svg);
  const pngData = resvg.render();
  const pngBuffer = pngData.asPng();

  writeFileSync('./wip/test-satori.png', pngBuffer);
  console.log('Generated: wip/test-satori.png');
  console.log('Size:', pngBuffer.length, 'bytes');
}

generateOgImage().catch(console.error);
