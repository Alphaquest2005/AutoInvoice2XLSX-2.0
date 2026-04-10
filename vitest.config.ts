import { defineConfig } from 'vitest/config';
import path from 'path';

export default defineConfig({
  test: {
    globals: true,
    include: ['app/tests/**/*.test.ts'],
    environment: 'node',
  },
  resolve: {
    alias: {
      '@shared': path.resolve(__dirname, 'app/shared'),
    },
  },
});
