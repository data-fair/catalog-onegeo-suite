import type { CatalogPlugin, GetResourceContext } from '@data-fair/types-catalogs'
import type { OneGeoSuiteConfig, Link } from '#types'
import { apiList, formatsList, sortList } from './list.ts'

import axios from '@data-fair/lib-node/axios.js'

export const getResource = async ({ catalogConfig, importConfig, resourceId, tmpDir, log }: GetResourceContext<OneGeoSuiteConfig>): ReturnType<CatalogPlugin['getResource']> => {
  let service: string = importConfig.service
  let format: string = service ? importConfig.format : (importConfig.format2 || undefined)
  const catalog = (await axios.get(new URL(`fr/indexer/elastic/_search/?q=uuid.keyword:${resourceId}%20AND%20is_metadata:true`, catalogConfig.url).href)).data.hits.hits[0]
  if (!format) {
    if (!service) {
      const links = catalog._source['metadata-fr'].link.filter((x: any) => { return apiList.includes(x.service) && x.formats.find((y: string) => { return formatsList.includes(y) }) })
      service = sortList(links.map((x: Link) => x.service), apiList)[0]
    }
    format = sortList(catalog._source['metadata-fr'].link.find((x: Link) => { return (x.service === service || x.url === service) && x.formats.find((y: string) => { return formatsList.includes(y) }) }).formats, formatsList)[0]
    if (!format) throw Error(`resource not found for service ${service}`)
  } else {
    if (!service) {
      const links = catalog._source['metadata-fr'].link.filter((x: Link) => { return x.formats.includes(format) })
      service = sortList(links.map((x: Link) => x.service), apiList)[0]
      if (!service) throw Error(`resource not found for format ${format}`)
    }
  }

  if (!catalog) throw Error(`resource not found for ${resourceId} in ${catalogConfig.url}`)
  if (!service) throw Error('resource not found')
  if (!format) throw Error(`resource not found for service ${service}`)

  // filter links by format and service
  const source: Link = catalog._source['metadata-fr'].link.find((x: Link) => {
    return x.service === service || x.url === service
  })
  if (!source) throw Error('resource not found')
  // table of format for make WFS url
  const wfsTable: Record<string, string> = {
    CSV: 'csv',
    JSON: 'application/json',
    GeoJSON: 'application/json',
    'Shapefile (zip)': 'SHAPE-ZIP',
    'SHAPE-ZIP': 'SHAPE-ZIP',
    KML: 'kml',
  }
  // table of format
  const extensionTable: Record<string, string> = {
    CSV: '.csv',
    GeoJSON: '.geojson',
    JSON: '.json',
    'Shapefile (zip)': '.zip',
    'SHAPE-ZIP': '.zip',
    KML: '.kml',
    'Excel non structuré': '.xlsx',
    'Microsoft Excel': '.xls',
  }

  let downloadUrl: string
  if (source.service === 'WS') {
    if (extensionTable[format] === undefined) throw Error(`Format ${format} not valid for ${service}`)
    downloadUrl = `${source.url}/${source.name}/all${extensionTable[format]}`
  } else if (source.service === undefined) {
    downloadUrl = `${source.url}`
  } else if (source.service === 'WFS') {
    if (wfsTable[format] === undefined) throw Error(`Format ${format} not valid for ${service}`)
    downloadUrl = `${source.url}?SERVICE=WFS&VERSION=2.0.0&request=GetFeature&typename=${source.name}&outputFormat=${wfsTable[format]}`
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

  const FREQUENCY_VALUES = [
    '',
    'triennial',
    'biennial',
    'annual',
    'semiannual',
    'threeTimesAYear',
    'quarterly',
    'bimonthly',
    'monthly',
    'semimonthly',
    'biweekly',
    'threeTimesAMonth',
    'weekly',
    'semiweekly',
    'threeTimesAWeek',
    'daily',
    'continuous',
    'irregular'
  ]

  let frequency = catalog._source['metadata-fr'].updateFrequency ?? ''
  if (!FREQUENCY_VALUES.includes(frequency)) frequency = ''

  return {
    id: resourceId,
    slug: catalog._source.slug,
    title: catalog._source['metadata-fr'].title,
    description: source.description ?? catalog._source['metadata-fr'].abstratc,
    filePath,
    format,
    frequency,
    license: {
      href: '',
      title: catalog._source['metadata-fr'].license
    },
    keywords: catalog._source['metadata-fr'].keyword,
    updatedAt: catalog._source['metadata-fr'].lastUpdateDate ?? undefined,
    image: catalog._source['metadata-fr'].image.find((x: { type: string, url: string | null }) => { return x.type === 'thumbnail' && !!x.url })?.url ?? null,
    origin
  }
}
