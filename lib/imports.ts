import type { CatalogPlugin, GetResourceContext } from '@data-fair/types-catalogs'
import type { MockConfig } from '#types'

import axios from '@data-fair/lib-node/axios.js'

const baseReqResource = (id: string) => {
  return {
    from: 0,
    size: 1,
    _source: ['data-fr', 'is_metadata', 'metadata-fr', 'editorial-metadata', 'uuid', 'fields', 'slug', 'extras'],
    track_total_hits: true,
    query: { bool: { must: [{ bool: { should: [{ term: { 'uuid.keyword': id } }] } }, { term: { is_metadata: true } }] } }
  }
}

export const getResource = async ({ catalogConfig, secrets, resourceId, tmpDir, log }: GetResourceContext<MockConfig>): ReturnType<CatalogPlugin['getResource']> => {
  const parts = resourceId.split(':')
  if (parts.length !== 3) {
    throw new Error(`Invalid resource ID format: ${resourceId}. Expected: "datasetId:resourceId"`)
  }
  const [datasetId, service, format] = parts

  const catalog = (await axios.post(new URL('fr/indexer/elastic/_search/', catalogConfig.url).href, baseReqResource(datasetId))).data.hits.hits[0]
  const resource = catalog._source['metadata-fr'].link.find((x: any) => { return x.service === service })

  const downloadUrl = `${resource.url}/${resource.name}/all.${format}`

  await log.step('Downloading the file')
  // Download the resource
  const fs = await import('node:fs')
  const path = await import('path')

  const response = await axios.get(downloadUrl, {
    responseType: 'stream'
  })

  // Create a filename
  const fileName = catalog._source.slug
  const filePath = path.join(tmpDir, fileName)
  await log.info(`Downloading resource to ${fileName}`)

  // Create write stream
  const writeStream = fs.createWriteStream(filePath)
  response.data.pipe(writeStream)

  // Return a promise that resolves with the file path
  await new Promise((resolve, reject) => {
    writeStream.on('finish', () => resolve(filePath))
    writeStream.on('error', (error) => reject(error))
  })
  await log.info(`Resource ${fileName} downloaded successfully!`)

  return {
    id: resourceId,
    title: catalog.title,
    description: resource.description + '\n\n' + secrets.secretField, // Include the secret in the description for demonstration
    filePath,
    format,
    frequency: catalog.updateFrequency,
    image: '',
    license: {
      href: '/',
      title: catalog.license
    },
    keywords: catalog.keyword,
    origin: ''
  }
}
