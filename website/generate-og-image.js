import { chromium } from 'playwright';
import { fileURLToPath } from 'url';
import { dirname, join } from 'path';

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

async function generateOgImage() {
    const browser = await chromium.launch();
    const page = await browser.newPage();

    // Set viewport to OG image dimensions
    await page.setViewportSize({ width: 1200, height: 630 });

    // Load the OG image template
    const templatePath = join(__dirname, 'og-image.html');
    await page.goto(`file://${templatePath}`);

    // Wait for fonts to load
    await page.waitForTimeout(1000);

    // Take screenshot
    const outputPath = join(__dirname, 'og-image.png');
    await page.screenshot({
        path: outputPath,
        type: 'png',
    });

    await browser.close();

    console.log(`Generated OG image: ${outputPath}`);
}

generateOgImage().catch(console.error);
