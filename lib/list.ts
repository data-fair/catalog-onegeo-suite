import type { CatalogPlugin, ListContext } from '@data-fair/types-catalogs'
import type { OneGeoSuiteConfig } from '#types'
import type { OneGeoCapabilities } from './capabilities.ts'
import axios from '@data-fair/lib-node/axios.js'

type ResourceList = Awaited<ReturnType<CatalogPlugin['list']>>['results']

const apiList = ['WS', undefined]
const formatsList = [
  'CSV', 'ODS', 'Excel non structuré', 'Microsoft Excel',
  'ZIP', 'Shapefile (zip)', 'GeoJSON', 'JSON', 'XML']

type link = {
  _main: boolean,
  name: string,
  description?: string,
  formats: Array<string>,
  service?: string
}

const baseReqDataset = (input: string = '*') => {
  return {
    from: 0,
    size: 10000,
    track_total_hits: true,
    query: {
      bool: {
        must: [
          {
            bool: {
              should: [{
                query_string: {
                  query: input,
                  fields: ['data_and_metadata', 'metadata-fr.title^5', 'metadata-fr.abstract^3', 'content-fr.title^5', 'content-fr.excerpt^3', 'content-fr.plaintext'],
                  analyzer: 'my_search_analyzer',
                  fuzziness: 'AUTO',
                  minimum_should_match: '90%',
                  default_operator: 'AND',
                  boost: 5
                }
              }]
            }
          }],
        must_not: { term: { 'content-fr.status.keyword': 'draft' } }
      }
    },
    sort: [{ 'metadata-fr.publicationDate': { order: 'desc', unmapped_type: 'date' } }],
    _source: { exclude: ['_dataset'] },
    collapse: {
      field: 'uuid.keyword'
    },
    post_filter: { terms: { 'type.keyword': ['dataset', 'nonGeographicDataset'] } }
  }
}

export const list = async ({ catalogConfig, params }: ListContext<OneGeoSuiteConfig, OneGeoCapabilities>): ReturnType<CatalogPlugin['list']> => {
  const url = catalogConfig.url

  const getBestFormat = (formats: string[]): number => {
    const sorted = [...formats].sort((a, b) =>
      (formatsList.indexOf(a) === -1 ? formats.length : formatsList.indexOf(a)) -
      (formatsList.indexOf(b) === -1 ? formats.length : formatsList.indexOf(b))
    )
    const best = sorted[0]
    const idx = formatsList.indexOf(best)
    return idx === -1 ? formatsList.length : idx
  }

  const listResources = async (params: Record<any, any>) => {
    const catalogs = (await axios.post(new URL('fr/indexer/elastic/_search/', url).href, baseReqDataset(params.q))).data.hits.hits

    const res = []
    for (const catalog of catalogs) {
      const resource = catalog._source['metadata-fr']
      const sources: Array<link> = resource.link
        .filter((x: link) => { return apiList.includes(x.service) })
        .filter((x: link) => { return x.formats.find((y: string) => { return formatsList.includes(y) }) })
      // sort source by priority (services / format)
      sources.sort((x: link, y: link) => {
        const bestFormatX = getBestFormat(x.formats)
        const bestFormatY = getBestFormat(y.formats)

        if (bestFormatX !== bestFormatY) {
          return bestFormatX - bestFormatY
        }

        return apiList.indexOf(x.service) - apiList.indexOf(y.service)
      })

      if (sources.length === 0) continue

      const formats: Array<string> = sources[0].formats
      formats.sort((a: string, b: string) => {
        return (formatsList.indexOf(a) === -1 ? formats.length : formatsList.indexOf(a)) - (formatsList.indexOf(b) === -1 ? formats.length : formatsList.indexOf(b))
      })

      if (formats.length === 0) continue

      let title = catalog._source['metadata-fr'].title
      if (sources[0].service) title += ` - ( ${sources[0].service} )`

      res.push({
        id: `${catalog._source.uuid}`,
        title,
        description: sources[0].description,
        format: formats[0],
        origin: '',
        type: 'resource'
      } as ResourceList[number])
    }
    return res
  }
  // List datasets
  let resources = await listResources(params)
  if (params.page && params.size) {
    resources = resources.slice((params.page - 1) * params.size, params.page * params.size)
  }

  return {
    count: resources.length,
    results: resources,
    path: []
  }
}
