import type { CatalogPlugin, ListContext } from '@data-fair/types-catalogs'
import type { OneGeoSuiteConfig } from '#types'
import type { OneGeoCapabilities } from './capabilities.ts'
import axios from '@data-fair/lib-node/axios.js'

type ResourceList = Awaited<ReturnType<CatalogPlugin['list']>>['results']

const apiList: Array<string | undefined> = ['WS', 'AFS', 'WFS', undefined]
const formatsList = [
  'CSV', 'ODS', 'Excel non structuré', 'Microsoft Excel',
  'ZIP', 'Shapefile (zip)', 'GeoJSON', 'JSON', 'XML', 'GML', 'KML']

export type link = {
  _main: boolean,
  name: string,
  description?: string,
  formats: string[],
  service?: string,
  url: string,
  projections?: string[]
}

const getBestFormat = (formats: string[]) => {
  return [...formats].sort((a, b) =>
    (formatsList.indexOf(a) === -1 ? formatsList.length : formatsList.indexOf(a)) -
    (formatsList.indexOf(b) === -1 ? formatsList.length : formatsList.indexOf(b))
  )
}
const baseReqDataset = (input: string = '*', size: number = 100000, from: number = 1) => {
  return {
    from: (from - 1) * size,
    size: Math.min(size, 10000),
    track_total_hits: true,
    query: {
      bool: {
        must: [{
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
        }, { term: { is_metadata: true } }, {
          bool: {
            should: [
              ...apiList.filter((x: any) => { return x }).map((x: any) => { return { term: { 'metadata-fr.link.service.keyword': x } } }),
              ...formatsList.filter((x: any) => { return x }).map((x: any) => { return { term: { 'metadata-fr.link.formats.keyword': x } } })],
          }
        }],
        must_not: [{ term: { 'content-fr.status.keyword': 'draft' } }]
      }
    },
    _source: { exclude: ['_dataset'] },
    collapse: {
      field: 'uuid.keyword'
    },
    post_filter: { terms: { 'type.keyword': ['dataset', 'nonGeographicDataset'] } }
  }
}

export const list = async ({ catalogConfig, params }: ListContext<OneGeoSuiteConfig, OneGeoCapabilities>): ReturnType<CatalogPlugin['list']> => {
  const url = catalogConfig.url
  const listResources = async (params: Record<any, any>) => {
    const catalogs = (await axios.post(new URL('fr/indexer/elastic/_search/', url).href, baseReqDataset(params.q, params.size, params.page))).data.hits.hits
    const res = []

    for (const catalog of catalogs) {
      const sources: Array<link> = catalog._source['metadata-fr'].link

      // sort source by priority (services / format)
      sources.sort((x: link, y: link) => {
        const bestFormatX = formatsList.indexOf(getBestFormat(x.formats)[0]) === -1 ? formatsList.length : formatsList.indexOf(getBestFormat(x.formats)[0])
        const bestFormatY = formatsList.indexOf(getBestFormat(y.formats)[0]) === -1 ? formatsList.length : formatsList.indexOf(getBestFormat(y.formats)[0])

        if (bestFormatX !== bestFormatY) {
          return bestFormatX - bestFormatY
        }

        return apiList.indexOf(x.service) - apiList.indexOf(y.service)
      })

      const formats: Array<string> = sources[0].formats
      formats.sort((a: string, b: string) => {
        return (formatsList.indexOf(a) === -1 ? formats.length : formatsList.indexOf(a)) - (formatsList.indexOf(b) === -1 ? formats.length : formatsList.indexOf(b))
      })

      let title = catalog._source['metadata-fr'].title
      if (sources[0].service) title += ` - ( ${sources[0].service} )`

      res.push({
        id: `${catalog._id}`,
        title,
        description: sources[0].description,
        format: formats[0],
        type: 'resource'
      } as ResourceList[number])
    }
    return res
  }
  // List datasets
  const resources = await listResources(params)

  return {
    count: resources.length,
    results: resources,
    path: []
  }
}
