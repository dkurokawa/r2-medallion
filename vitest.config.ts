import { defineConfig } from 'vitest/config';
import path from 'path';

export default defineConfig({
  resolve: {
    alias: {
      // Runtime-only module with no node implementation — needed by any test that transitively imports src/workflow.ts,
      // since that extends `WorkflowEntrypoint` from it at module scope.
      'cloudflare:workers': path.resolve(__dirname, './vitest.mock.cloudflare-workers.ts'),
    },
  },
  test: {
    environment: 'node',
    globals: false,
    include: ['src/**/__tests__/**/*.test.ts'],
  },
});
