import type CatalogPlugin from '@data-fair/types-catalogs'
import { configSchema, assertConfigValid, type OneGeoSuiteConfig } from '#types'
import { type OneGeoCapabilities, capabilities } from './lib/capabilities.ts'
import i18n from './lib/i18n.ts'

// Since the plugin is very frequently imported, each function is imported on demand,
// instead of loading the entire plugin.
// This file should not contain any code, but only constants and dynamic imports of functions.

const plugin: CatalogPlugin<OneGeoSuiteConfig, OneGeoCapabilities> = {
  async prepare (context) {
    const prepare = (await import('./lib/prepare.ts')).default
    return prepare(context)
  },

  async list (context) {
    const { list } = await import('./lib/list.ts')
    return list(context)
  },

  async getResource (context) {
    const { getResource } = await import('./lib/imports.ts')
    return getResource(context)
  },

  metadata: {
    title: 'OneGeoSuite',
    thumbnailPath: './lib/resources/thumbnail.svg',
    i18n,
    capabilities
  },

  configSchema,
  assertConfigValid
}
export default plugin
