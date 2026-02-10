import type { CatalogPlugin, GetResourceContext } from '@data-fair/types-catalogs'
import type { OneGeoSuiteConfig, Link } from '#types'
import { apiList, formatsList, sortList } from './list.ts'

import axios from '@data-fair/lib-node/axios.js'

// table of format -> extension
const wfsTable: Record<string, string> = {
  CSV: 'csv',
  JSON: 'application/json',
  GeoJSON: 'application/json',
  'Shapefile (zip)': 'SHAPE-ZIP',
  'SHAPE-ZIP': 'SHAPE-ZIP',
  KML: 'kml',
}

// table of format -> extension
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

export const getResource = async ({
  catalogConfig,
  resourceId,
  tmpDir,
  log
}: GetResourceContext<OneGeoSuiteConfig>): ReturnType<CatalogPlugin['getResource']> => {
  const catalog = (await axios.get(new URL(`fr/indexer/elastic/_search/?q=uuid.keyword:${resourceId}%20AND%20is_metadata:true`, catalogConfig.url).href)).data.hits.hits[0]
  if (!catalog) throw Error(`resource not found for ${resourceId} in ${catalogConfig.url}`)

  // get origine url
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

  const links: Link[] = catalog._source['metadata-fr'].link.filter((x: Link) => {
    return apiList.includes(x.service) && x.formats.find((y: string) => {
      return formatsList.includes(y)
    })
  })

  // list all url possible
  let downloadUrls: { url: string, format: string, service: string | undefined, description: string | undefined }[] = []

  for (const link of links) {
    const formats = link.formats.filter((f: string) => {
      return formatsList.includes(f)
    })
    for (const format of formats) {
      if (link.service === 'WS' && extensionTable[format]) {
        downloadUrls.push({ url: `${link.url}/${link.name}/all${extensionTable[format]}`, format, service: link.service, description: link.description })
      } else if (link.service === undefined) {
        downloadUrls.push({ url: `${link.url}`, format, service: link.service, description: link.description })
      } else if (link.service === 'WFS' && wfsTable[format]) {
        downloadUrls.push({ url: `${link.url}?SERVICE=WFS&VERSION=2.0.0&request=GetFeature&typename=${link.name}&outputFormat=${wfsTable[format]}`, format, service: link.service, description: link.description })
      }
    }
  }
  downloadUrls = sortList(downloadUrls, apiList, (x: any) => { return x.service })
  downloadUrls = sortList(downloadUrls, formatsList, (x: any) => { return x.format })

  // Download the resource
  const fs = await import('node:fs')
  const path = await import('path')
  let response
  let format: string
  let description: string | undefined

  for (const downloadUrl of downloadUrls) {
    await log.step(`Downloading the file ${downloadUrl.url}; format: ${downloadUrl.format}; service: ${downloadUrl.service}`)

    try {
      response = await axios.get(downloadUrl.url, {
        responseType: 'stream',
      })
      if (response.headers['content-type'] === 'text/html') {
        response = undefined
        throw Error('return HTML page')
      }
      format = downloadUrl.format
      description = downloadUrl.description
      await log.info(`Get file with ${downloadUrl.url} successfully! ${response.status}`)
    } catch (e) {
      await log.warning(`Downloading fail with this url: ${downloadUrl}; ${e}`)
    }
    if (response) break
  }

  if (!response) {
    throw Error(`Download failed ${origin}`)
  }

  // Create a filename
  const fileName = catalog._source.slug + extensionTable[format!]
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
    description: description ?? catalog._source['metadata-fr'].abstratc,
    filePath,
    format: format!,
    frequency,
    license: {
      href: '',
      title: catalog._source['metadata-fr'].license
    },
    keywords: catalog._source['metadata-fr'].keyword,
    updatedAt: catalog._source['metadata-fr'].lastUpdateDate ?? undefined,
    image: catalog._source['metadata-fr'].image.find((x: { type: string, url: string | null }) => {
      return x.type === 'thumbnail' && !!x.url
    })?.url ?? null,
    origin
  }
}
