import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';
import electron from 'vite-plugin-electron';
import electronRenderer from 'vite-plugin-electron-renderer';
import path from 'path';

export default defineConfig({
  plugins: [
    react(),
    electron([
      {
        entry: 'main/index.ts',
        vite: {
          build: {
            outDir: 'dist-electron/main',
            lib: {
              formats: ['cjs'],
            },
            rollupOptions: {
              external: ['better-sqlite3', 'chokidar', 'electron'],
              output: {
                format: 'cjs',
              },
            },
          },
        },
      },
      {
        entry: 'preload/index.ts',
        onstart(options) {
          options.reload();
        },
      },
    ]),
    electronRenderer(),
  ],
  resolve: {
    alias: {
      '@': path.resolve(__dirname, 'renderer'),
      '@shared': path.resolve(__dirname, 'shared'),
    },
  },
  build: {
    outDir: 'dist',
    emptyOutDir: true,
    rollupOptions: {
      input: 'index.html',
    },
  },
});
