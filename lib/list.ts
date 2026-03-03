import type { CatalogPlugin, Folder, ListContext } from '@data-fair/types-catalogs'
import type { Link, OneGeoSuiteConfig } from '#types'
import type { OneGeoCapabilities } from './capabilities.ts'
import axios from '@data-fair/lib-node/axios.js'
import { createOneGeoClient } from './onegeo-client.ts'

type ResourceList = Awaited<ReturnType<CatalogPlugin['list']>>['results']

export const apiList: Array<string | undefined> = ['WS', 'WFS', undefined]
export const formatsList = ['GeoJSON', 'SHAPE-ZIP', 'Shapefile (zip)', 'CSV', 'JSON', 'Excel non structuré', 'Microsoft Excel', 'KML']

export const extensionTable: Record<string, string> = {
  CSV: '.csv',
  GeoJSON: '.geojson',
  JSON: '.json',
  'Shapefile (zip)': '.zip',
  'SHAPE-ZIP': '.zip',
  KML: '.kml',
  'Excel non structuré': '.xlsx',
  'Microsoft Excel': '.xls',
}

export const sortList = (list: any[], reference: any[], func = (x: any) => {
  return x
}) => {
  return [...list].sort((a, b) =>
    (reference.indexOf(func(a)) === -1 ? reference.length : reference.indexOf(func(a))) -
    (reference.indexOf(func(b)) === -1 ? reference.length : reference.indexOf(func(b)))
  )
}

export const filterList = (list: any[], reference: any[], func = (x: any) => {
  return x
}) => {
  return [...list].filter((a: any) => reference.includes(func(a)))
}

export const sortFilterList = (list: any[], reference: any[], func = (x: any) => {
  return x
}) => {
  return sortList(filterList(list, reference, func), reference, func)
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
  secrets,
  params
}: ListContext<OneGeoSuiteConfig, OneGeoCapabilities>): ReturnType<CatalogPlugin['list']> => {
  const url = catalogConfig.url
  if (params.action) { // Publication flow
    if (!secrets?.username || !secrets?.password) {
      throw new Error('Un nom d\'utilisateur et un mot de passe sont requis pour lister les jeux de données en vue d\'une publication')
    }
    const client = createOneGeoClient(url, secrets)

    if (params.currentFolderId) {
      const response = await client.request({
        method: 'GET',
        url: 'resource/resource-dataset/',
        params: {
          dataset_id: params.currentFolderId
        }
      })

      const liaisons = response.data.results || response.data

      const resources = liaisons.map((liaison: any) => ({
        id: `${params.currentFolderId}:${liaison.resource.id}`,
        title: liaison.resource?.display_name || liaison.resource?.codename || 'Ressource',
        type: 'resource'
      }))

      return {
        count: resources.length,
        results: resources,
        path: [{
          id: String(params.currentFolderId),
          title: 'Jeu de données',
          type: 'folder'
        }]
      }
    }

    const requestParams: Record<string, any> = {
      page: params.page || 1,
      page_size: params.size || 20
    }
    if (params.q && params.q !== '*') requestParams.search = params.q

    const response = await client.request({
      method: 'GET',
      url: 'dataset/datasets/',
      params: requestParams
    })
    const datasets = response.data.results || response.data
    const count = response.data.count || datasets.length

    const folders = datasets.map((ds: any) => ({
      id: String(ds.id),
      title: ds.display_name || ds.title || ds.codename,
      type: 'folder',
      updatedAt: ds.last_update_date || ds.last_revision_date
    } as Folder))

    return {
      count,
      results: folders,
      path: []
    }
  }
  const listResources = async (params: Record<any, any>) => {
    // get resources
    let resources
    try {
      resources = (await axios.post(new URL('indexer/elastic/_search/', url).href, baseReqDataset(params.q || '*', params.size, params.page))).data.hits.hits
    } catch (e) {
      // @ts-ignore
      throw Error(`Axios error: ${e?.status ?? ''} ${e?.message}`)
    }
    const count = (await axios.post(new URL('indexer/elastic/_search/', url).href, countReq(params.q))).data.aggregations.unique_datasets.value
    const res = []

    for (const catalog of resources) {
      // get sources
      const sources: Link[] = catalog._source['metadata-fr'].link
      // get all formats possible
      const formats: string[] = sortFilterList([...(new Set(sources.map((x: Link) => {
        return x.formats
      }).flat()))], formatsList).map((f: string) => extensionTable[f].slice(1))

      res.push({
        id: `${catalog._source.uuid}`,
        title: catalog._source['metadata-fr'].title,
        description: sources[0].description,
        format: formats.slice(0, 3).join(', ') + (formats.length > 3 ? '..' : ''),
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
