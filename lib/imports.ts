import type { CatalogPlugin, GetResourceContext } from '@data-fair/types-catalogs'
import type { OneGeoSuiteConfig } from '#types'
import { type link } from './list.ts'

import axios from '@data-fair/lib-node/axios.js'

export const getResource = async ({ catalogConfig, importConfig, resourceId, tmpDir, log }: GetResourceContext<OneGeoSuiteConfig>): ReturnType<CatalogPlugin['getResource']> => {
  const format: string = importConfig.format
  const service: string = importConfig.service
  const catalog = (await axios.get(new URL(`fr/indexer/elastic/_search/?q=_id:${resourceId}`, catalogConfig.url).href)).data.hits.hits[0]

  if (!catalog) {
    throw Error(`resource ${service} not found for ${resourceId} in ${catalogConfig.url}`)
  }

  // filter links by format and service
  const source: link = catalog._source['metadata-fr'].link.find((x: link) => {
    return x.service === service || x.url === service
  })

  // table of format for make AFS url
  const afsTable: Record<string, string> = {
    CSV: 'text/csv',
    JSON: 'application/json',
    GeoJSON: 'application/Geo%2Bjson',
    GML: 'application/gml%2Bxml',
    KML: 'application/vnd.google-earth.kml%2Bxml',
  }
  // table of format for make WFS url
  const wfsTable: Record<string, string> = {
    CSV: 'csv',
    GeoJSON: 'application/json',
    'Shapefile (zip)': 'SHAPE-ZIP',
    GML: 'GML3',
    KML: 'kml',
  }
  // table of format
  const extensionTable: Record<string, string> = {
    CSV: '.csv',
    GeoJSON: '.geojson',
    JSON: '.json',
    'Shapefile (zip)': '.zip',
    ZIP: '.zip',
    GML: '.gml',
    KML: '.kml',
    XML: '.xml',
    ODS: '.ods',
    'Excel non structuré': '.xlsx',
    'Microsoft Excel': '.xls',
  }

  let downloadUrl: string
  if (source.service === 'WS') {
    downloadUrl = `${source.url}/${source.name}/all${extensionTable[format]}`
  } else if (source.service === undefined) {
    downloadUrl = `${source.url}`
  } else if (source.service === 'AFS') {
    downloadUrl = `${source.url}${source.name}/items?&crs=${source.projections![0]}&f=${afsTable[format]}&sortby=gid`
  } else if (source.service === 'WFS') {
    downloadUrl = `${source.url}?SERVICE=WFS&VERSION=2.0.0&request=GetFeature&typename=${source.name}&outputFormat=${wfsTable[format]}&startIndex=0&sortby=gid`
  } else {
    downloadUrl = `${source.url}`
  }

  await log.step(`Downloading the file ${downloadUrl}`)

  // Download the resource
  const fs = await import('node:fs')
  const path = await import('path')

  const axiosPortail = axios.create({
    validateStatus: function (status) {
      return status >= 200 && status < 500
    }
  })

  let portail
  if ([200].includes((await axiosPortail.get(new URL('explorer/fr', catalogConfig.url).href)).status)) {
    portail = 'explorer/fr'
  } else if ([200].includes((await axiosPortail.get(new URL('portail/fr', catalogConfig.url).href)).status)) {
    portail = 'portail/fr'
  }
  const origin = portail ? `${catalogConfig.url}/${portail}/jeux-de-donnees/${catalog._source.slug}/info` : ''

  let response
  try {
    response = await axios.get(downloadUrl, {
      responseType: 'stream',
    })
    if (response.headers['content-type'] === 'text/html') {
      response = undefined
      throw Error('return HTML page')
    }

    await log.info(`Get file with ${downloadUrl} successfully! ${response.status}`)
  } catch (e) {
    await log.warning(`Get file fail with this url: ${downloadUrl}; ${e}`)
  }

  if (!response) {
    throw Error(`Download failed ${origin}`)
  }

  // Create a filename
  const fileName = catalog._source.slug + extensionTable[format]
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
    description: source.description ?? catalog._source['metadata-fr'].abstratc,
    filePath,
    format,
    frequency: catalog._source['metadata-fr'].updateFrequency ?? '',
    license: {
      href: '',
      title: catalog._source['metadata-fr'].license
    },
    keywords: catalog._source['metadata-fr'].keyword,
    updatedAt: catalog._source['metadata-fr'].lastUpdateDate ?? undefined,
    image: catalog._source['metadata-fr'].image.find((x: { type: string, url: string | null }) => { return x.type === 'thumbnail' && !!x.url }),
    origin
  }
}
