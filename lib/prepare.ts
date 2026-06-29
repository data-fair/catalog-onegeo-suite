import type { OneGeoSuiteConfig } from '#types'
import type { OneGeoCapabilities } from './capabilities.ts'
import type { PrepareContext } from '@data-fair/types-catalogs'
import axios from '@data-fair/lib-node/axios.js'

export default async ({ catalogConfig, capabilities, secrets }: PrepareContext<OneGeoSuiteConfig, OneGeoCapabilities>) => {
  if (!catalogConfig.url) {
    throw new Error('Catalog configuration is missing the "url" property.')
  }

  const password = catalogConfig.password
  if (password && password !== '**************************************************') {
    secrets.password = password
    catalogConfig.password = '**************************************************'
  } else if (secrets?.password && password === '') {
    delete secrets.password
  }

  const username = catalogConfig.username
  if (username) {
    secrets.username = username
  } else {
    delete secrets.username
  }

  const publicationCapabilities = ['createFolderInRoot', 'createResource', 'replaceFolder', 'replaceResource'] as const
  if (secrets?.username && secrets?.password) {
    for (const cap of publicationCapabilities) {
      if (!capabilities.includes(cap as any)) capabilities.push(cap as any)
    }
  } else {
    capabilities = capabilities.filter(c => !publicationCapabilities.includes(c as any))
  }

  capabilities = capabilities.filter(c => c !== 'publication' as any)
  if (!capabilities.includes('requiresPublicationSite')) capabilities.push('requiresPublicationSite')
  try {
    if (secrets?.username && secrets?.password) {
      if (catalogConfig.usergroup?.id === undefined) {
        throw new Error('An organization is required to publish to OneGeo Suite. Please add an organization in the catalog configuration.')
      }
      await axios.post(`${catalogConfig.url}/login/signin/`, {
        username: secrets.username,
        password: secrets.password
      })
    } else {
      await axios.get(`${catalogConfig.url}`)
    }
  } catch (error: any) {
    console.error(`Error connecting to OneGeoSuite API at ${catalogConfig.url}:`, error.message)
    if (error.response?.status === 401 || error.response?.status === 403) {
      throw new Error('Invalid username or password on OneGeo Suite.')
    }
    throw new Error(`Unable to connect to the OneGeo Suite API (${catalogConfig.url}). Check the URL and your network connection.`)
  }

  return {
    catalogConfig,
    capabilities,
    secrets
  }
}
