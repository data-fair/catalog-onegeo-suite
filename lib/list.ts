import type { CatalogPlugin, ListContext } from '@data-fair/types-catalogs'
import type { MockConfig } from '#types'
import type { MockCapabilities } from './capabilities.ts'
import axios from '@data-fair/lib-node/axios.js'

type ResourceList = Awaited<ReturnType<CatalogPlugin['list']>>['results']

const baseReqDataset = (input: string = '*', size: number = 50, from: number = 1) => {
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
        }, { term: { is_metadata: true } }],
        must_not: { term: { 'content-fr.status.keyword': 'draft' } }
      }
    },
    _source: { exclude: ['_dataset'] },
    collapse: {
      field: 'uuid.keyword'
    },
    post_filter: { terms: { 'type.keyword': ['dataset', 'nonGeographicDataset'] } }
  }
}

export const list = async ({ catalogConfig, secrets, params }: ListContext<MockConfig, MockCapabilities>): ReturnType<CatalogPlugin['list']> => {
  const url = catalogConfig.url

  const listResources = async (params: Record<any, any>) => {
    const catalogs = (await axios.post(new URL('fr/indexer/elastic/_search/', url).href, baseReqDataset(params.q, params.size, params.page))).data.hits.hits
    const res = []

    for (const catalog of catalogs) {
      const apiList = ['WS', 'WFS', 'AFS', undefined]
      const formatsList = ['CSV']

      const resource = catalog._source['metadata-fr']

      const sources = resource.link.filter((x: any) => {
        return x._main && apiList.includes(x.service) && x.formats.filter((y: any) => {
          return formatsList.includes(y)
        }).length
      })

      // sort source by priority (services / format)
      sources.sort((x: any, y: any) => {
        // sort by format
        // sort format of x
        const ls: Array<string> = x.formats.filter((z: string) => {
          return formatsList.includes(z)
        })
        ls.sort((a: string, b: string) => {
          return (formatsList.indexOf(a) === -1 ? ls.length : formatsList.indexOf(a)) - (formatsList.indexOf(b) === -1 ? ls.length : formatsList.indexOf(b))
        })
        // sort format of y
        const ls2: Array<string> = y.formats.filter((z: string) => {
          return formatsList.includes(z)
        })
        ls.sort((a: string, b: string) => {
          return (formatsList.indexOf(a) === -1 ? ls.length : formatsList.indexOf(a)) - (formatsList.indexOf(b) === -1 ? ls.length : formatsList.indexOf(b))
        })

        const idX = formatsList.indexOf(ls[0])
        const idY = formatsList.indexOf(ls2[0])
        if (idX !== idY) {
          return idX - idY
        } else {
          // sort by service
          return apiList.indexOf(x) - apiList.indexOf(y)
        }
      })

      if (sources.length === 0) {
        continue
      }
      const formats: Array<string> = sources[0].formats.filter((z: string) => {
        return formatsList.includes(z)
      })
      formats.sort((a: string, b: string) => {
        return (formatsList.indexOf(a) === -1 ? formats.length : formatsList.indexOf(a)) - (formatsList.indexOf(b) === -1 ? formats.length : formatsList.indexOf(b))
      })
      let title = catalog._source['metadata-fr'].title
      if (sources[0].service) title += ` - ( ${sources[0].service} )`

      res.push({
        id: `${catalog._source.uuid}:${sources[0].service ?? ''}:${formats[0].toLowerCase()}`,
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
  const resources = await listResources(params)
  return {
    count: resources.length,
    results: resources,
    path: []
  }
}
