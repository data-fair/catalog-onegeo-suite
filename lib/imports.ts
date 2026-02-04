import type { CatalogPlugin, GetResourceContext } from '@data-fair/types-catalogs'
import type { OneGeoSuiteConfig } from '#types'

import axios from '@data-fair/lib-node/axios.js'

type link = {
  _main: boolean,
  name: string,
  description?: string,
  formats: Array<string>,
  service?: string,
  url?: string
}

const apiList = ['WS', undefined]
const formatsList = [
  'CSV', 'ODS', 'Excel non structuré', 'Microsoft Excel',
  'ZIP', 'Shapefile (zip)', 'GeoJSON', 'JSON', 'XML']

const getBestFormat = (formats: string[]): string[] => {
  return [...formats].sort((a, b) =>
    (formatsList.indexOf(a) === -1 ? formats.length : formatsList.indexOf(a)) -
    (formatsList.indexOf(b) === -1 ? formats.length : formatsList.indexOf(b))
  )
}

const baseReqResource = (id: string) => {
  return {
    from: 0,
    size: 1,
    _source: ['data-fr', 'is_metadata', 'metadata-fr', 'editorial-metadata', 'uuid', 'fields', 'slug', 'extras'],
    track_total_hits: true,
    query: { bool: { must: [{ bool: { should: [{ term: { 'uuid.keyword': id } }] } }, { term: { is_metadata: true } }] } }
  }
}

export const getResource = async ({ catalogConfig, importConfig, resourceId, tmpDir, log }: GetResourceContext<OneGeoSuiteConfig>): ReturnType<CatalogPlugin['getResource']> => {
  const catalog = (await axios.post(new URL('fr/indexer/elastic/_search/', catalogConfig.url).href, baseReqResource(resourceId))).data.hits.hits[0]
  const sources: Array<link> = catalog._source['metadata-fr'].link
    .filter((x: link) => { return apiList.includes(x.service) })
    .filter((x: link) => {
      return x.formats.find((y: string) => { return formatsList.includes(y) })
    })

  sources.sort((x: link, y: link) => {
    let bestFormatX = formatsList.indexOf(getBestFormat(x.formats)[0])
    bestFormatX = bestFormatX === -1 ? formatsList.length : bestFormatX

    let bestFormatY = formatsList.indexOf(getBestFormat(y.formats)[0])
    bestFormatY = bestFormatY === -1 ? formatsList.length : bestFormatY

    if (bestFormatX !== bestFormatY) {
      return bestFormatX - bestFormatY
    }

    return apiList.indexOf(x.service) - apiList.indexOf(y.service)
  })

  const downloadUrls = sources.map((x: link) => {
    if (x.service === 'WS') {
      return `${x.url}/${x.name}/all.${getBestFormat(x.formats)[0].toLowerCase()}`
    } else if (x.service === undefined) {
      return `${x.url}`
    }
    return ''
  })

  if (downloadUrls.length === 0) {
    throw Error('Download URL not found')
  }

  await log.step('Downloading the file')
  // Download the resource
  const fs = await import('node:fs')
  const path = await import('path')

  let downloadUrl
  let response
  for (downloadUrl of downloadUrls) {
    try {
      response = await axios.get(downloadUrl, {
        responseType: 'stream'
      })
      await log.info(`Get file with ${downloadUrl} successfully!`)
      break
    } catch (e) {
      await log.warning(`Get file fail with this url: ${downloadUrl}; ${e}`)
    }
  }
  if (!response) {
    throw Error('Download failed')
  }

  const format = getBestFormat(sources[downloadUrls.indexOf(downloadUrl!)].formats)[0]

  // Create a filename
  const fileName = catalog._source.slug + ''
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
    slug: catalog._source.slug,
    title: catalog._source['metadata-fr'].title,
    description: importConfig.useDatasetDescription ? catalog._source['metadata-fr'].abstratc : sources[downloadUrls.indexOf(downloadUrl!)].description,
    filePath,
    format,
    frequency: catalog._source['metadata-fr'].updateFrequency,
    license: {
      href: '',
      title: catalog._source['metadata-fr'].license
    },
    keywords: catalog._source['metadata-fr'].keyword,
    updatedAt: catalog._source['metadata-fr'].lastUpdateDate,
    image: catalog._source['metadata-fr'].image.find((x: { name: string, url: string | null }) => { return x.name === 'thumbnail' && x.url })
  }
}
