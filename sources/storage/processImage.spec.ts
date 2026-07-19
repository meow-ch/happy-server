import sharp from 'sharp';
import { processImage } from './processImage';
import { describe, expect, it } from 'vitest';

describe('processImage', () => {
    it('should resize image', async () => {
        const img = await sharp({
            create: {
                width: 200,
                height: 100,
                channels: 3,
                background: { r: 20, g: 40, b: 60 }
            }
        }).jpeg().toBuffer();

        const result = await processImage(img);

        expect(result).toMatchObject({ width: 200, height: 100, format: 'jpeg' });
        expect(result.pixels).toHaveLength(100 * 50 * 4);
        expect(result.thumbhash.length).toBeGreaterThan(0);
    });
});
