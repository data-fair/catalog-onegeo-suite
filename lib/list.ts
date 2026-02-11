import type { CatalogPlugin, ListContext } from '@data-fair/types-catalogs'
import type { Link, OneGeoSuiteConfig } from '#types'
import type { OneGeoCapabilities } from './capabilities.ts'
import axios from '@data-fair/lib-node/axios.js'

type ResourceList = Awaited<ReturnType<CatalogPlugin['list']>>['results']

export const apiList: Array<string | undefined> = ['WS', 'WFS', undefined]
export const formatsList = [
  'GeoJSON', 'SHAPE-ZIP', 'Shapefile (zip)', 'CSV', 'JSON', 'Excel non structuré', 'Microsoft Excel', 'KML']

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

export const sortList = (formats: any[], reference: any[], func = (x: any) => {
  return x
}) => {
  return [...formats].sort((a, b) =>
    (reference.indexOf(func(a)) === -1 ? reference.length : reference.indexOf(func(a))) -
    (reference.indexOf(func(b)) === -1 ? reference.length : reference.indexOf(func(b)))
  )
}
const baseReqDataset = (input: string = '*', size: number = 500, from: number = 1) => {
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
        }, { term: { is_metadata: true } }, { term: { 'editorial-metadata.defaultPermissionLevel': 3 } }, {
          bool: {
            should: [
              ...apiList.filter((x: any) => {
                return x
              }).map((x: any) => {
                return { term: { 'metadata-fr.link.service.keyword': x } }
              }),
              ...formatsList.filter((x: any) => {
                return x
              }).map((x: any) => {
                return { term: { 'metadata-fr.link.formats.keyword': x } }
              })],
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

const countReq = (input: string = '*') => {
  return {
    size: 0,
    track_total_hits:
      false,
    query:
      {
        bool: {
          must: [{
            bool: {
              should: [{
                query_string: {
                  query: input || '*',
                  fields: ['data_and_metadata', 'metadata-fr.title^5', 'metadata-fr.abstract^3', 'content-fr.title^5', 'content-fr.excerpt^3', 'content-fr.plaintext'],
                  analyzer: 'my_search_analyzer',
                  fuzziness: 'AUTO',
                  minimum_should_match: '90%',
                  default_operator: 'AND',
                  boost: 5
                }
              }]
            }
          }, { term: { is_metadata: true } }, { term: { 'editorial-metadata.defaultPermissionLevel': 3 } }, {
            bool: {
              should: [
                ...apiList.filter((x: any) => x).map((x: any) => ({ term: { 'metadata-fr.link.service.keyword': x } })),
                ...formatsList.filter((x: any) => x).map((x: any) => ({ term: { 'metadata-fr.link.formats.keyword': x } }))
              ],
            }
          }],
          must_not:
            [{ term: { 'content-fr.status.keyword': 'draft' } }],
          filter:
            {
              terms: {
                'type.keyword':
                  ['dataset', 'nonGeographicDataset']
              }
            }
        }
      },
    aggs: {
      unique_datasets: {
        cardinality: {
          field: 'uuid.keyword',
          precision_threshold:
            40000
        }
      }
    }
  }
}

export const list = async ({
  catalogConfig,
  params
}: ListContext<OneGeoSuiteConfig, OneGeoCapabilities>): ReturnType<CatalogPlugin['list']> => {
  const url = catalogConfig.url
  const listResources = async (params: Record<any, any>) => {
    // get resources
    let resources
    try {
      resources = (await axios.post(new URL('fr/indexer/elastic/_search/', url).href, baseReqDataset(params.q || '*', params.size, params.page))).data.hits.hits
    } catch (e) {
      // @ts-ignore
      throw Error(`Axios error: ${e?.status ?? ''} ${e?.message}`)
    }
    const count = (await axios.post(new URL('fr/indexer/elastic/_search/', url).href, countReq(params.q))).data.aggregations.unique_datasets.value
    const res = []

    for (const catalog of resources) {
      // get sources
      const sources: Link[] = catalog._source['metadata-fr'].link
      // get all formats possible
      const formats: string[] = sortList(new Array(...(new Set(sources.map((x: Link) => {
        return x.formats
      }).flat().filter((f: string) => formatsList.includes(f))))), formatsList).map((f: string) => extensionTable[f].slice(1))

      res.push({
        id: `${catalog._source.uuid}`,
        title: catalog._source['metadata-fr'].title,
        description: sources[0].description,
        format: formats.slice(0, 3).join(', ') + (formats.length ? '..' : ''),
        type: 'resource'
      } as ResourceList[number])
    }
    return [res, count]
  }
  const [resources, count] = await listResources(params)
  return {
    count,
    results: resources,
    path: []
  }
}
