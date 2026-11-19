import { resolve } from 'node:path'
import { defineConfig } from 'vite-plus'

export default defineConfig({
  pack: {
    entry: ['src/index.ts', 'src/miniflare.ts'],
    format: ['esm'],
    dts: true,
    sourcemap: true,
    clean: true,
    platform: 'node',
    target: 'node24',
    fixedExtension: false,
    deps: {
      neverBundle: ['miniflare'],
    },
    publint: true,
    attw: {
      profile: 'esm-only',
    },
    failOnWarn: true,
  },
  test: {
    include: ['test/**/*.test.mjs'],
    testTimeout: 30_000,
    alias: {
      'cloudflake/miniflare': resolve(import.meta.dirname, 'src/miniflare.ts'),
      cloudflake: resolve(import.meta.dirname, 'src/index.ts'),
    },
  },
  fmt: {
    semi: false,
    singleQuote: true,
    printWidth: 100,
    ignorePatterns: ['dist/**', 'package-lock.json'],
  },
  lint: {
    jsPlugins: [{ name: 'vite-plus', specifier: 'vite-plus/oxlint-plugin' }],
    rules: {
      'vite-plus/prefer-vite-plus-imports': 'error',
    },
    options: {
      typeAware: true,
      typeCheck: true,
    },
    ignorePatterns: ['dist/**'],
  },
  staged: {
    '*': 'vp check --fix',
  },
})
