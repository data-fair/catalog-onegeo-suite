import type { OneGeoSuiteConfig } from '#types'
import type { OneGeoCapabilities } from './capabilities.ts'
import type { PrepareContext } from '@data-fair/types-catalogs'
import axios from '@data-fair/lib-node/axios.js'

export default async ({ catalogConfig }: PrepareContext<OneGeoSuiteConfig, OneGeoCapabilities>) => {
  if (!catalogConfig.url) {
    throw new Error('Catalog configuration is missing the "url" property.')
  }

  try {
    await axios.get(`${catalogConfig.url}/fr/dataset`)
  } catch (error) {
    console.error(`Error connecting to OneGeoSuite API at ${catalogConfig.url}:`, error)
    throw new Error(`Unable to connect to OneGeoSuite API at ${catalogConfig.url}. Please check the URL and your network connection.`)
  }

  return {
    catalogConfig
  }
}
