import type { CatalogPlugin, Publication, PublishDatasetContext, DeletePublicationContext } from '@data-fair/types-catalogs'
import type { OneGeoSuiteConfig } from '#types'
import type { OneGeoCapabilities } from './capabilities.ts'
import type { OneGeoClient } from './onegeo-client.ts'

import { microTemplate } from '@data-fair/lib-utils/micro-template.js'
import { createOneGeoClient } from './onegeo-client.ts'

export const publishDataset = async (context: PublishDatasetContext<OneGeoSuiteConfig, OneGeoCapabilities>): ReturnType<CatalogPlugin['publishDataset']> => {
  if (!context.secrets?.username || !context.secrets?.password) {
    throw new Error('A username and password are required to publish to OneGeo Suite')
  }

  const oneGeoClient = createOneGeoClient(context.catalogConfig.url, context.secrets)

  if (['createResource', 'replaceResource'].includes(context.publication.action)) {
    return createOrUpdateResource(context, oneGeoClient)
  } else {
    return await createOrUpdateDataset(context, oneGeoClient)
  }
}

export const deletePublication = async (context: DeletePublicationContext<OneGeoSuiteConfig>): ReturnType<CatalogPlugin['deletePublication']> => {
  if (!context.secrets?.username || !context.secrets?.password) {
    throw new Error('A username and password are required to delete a publication on OneGeo Suite')
  }

  const oneGeoClient = createOneGeoClient(context.catalogConfig.url, context.secrets)

  if (context.resourceId) return await deleteResource(context, oneGeoClient)
  else await deleteDataset(context, oneGeoClient)
}

const createOrUpdateDataset = async ({ catalogConfig, dataset, publication, publicationSite, log }: PublishDatasetContext<OneGeoSuiteConfig, OneGeoCapabilities>, client: OneGeoClient): Promise<Publication> => {
  await log.step('Preparing the dataset for publication/update on OneGeo Suite')

  await log.step('Building OneGeo Suite metadata')
  const slug = dataset.id.replace(/[^a-zA-Z0-9_-]/g, '-').substring(0, 100)
  if (catalogConfig?.usergroup?.id === undefined) {
    await log.warning('No organization specified in the catalog configuration.')
    throw new Error('An organization is required to publish to OneGeo Suite. Please add an organization in the catalog configuration.')
  }
  const now = new Date().toISOString().split('T')[0]
  const onegeoDataset: Record<string, any> = {
    codename: slug,
    display_name: dataset.title,
    description: dataset.description || dataset.title,
    publish: dataset.public,
    usergroup: catalogConfig.usergroup.id,
    tags: dataset.tags || [],
    publication_date: (dataset.createdAt || '').split('T')[0] || now,
    last_update_date: (dataset.updatedAt || '').split('T')[0] || now,
  }

  if (publication.remoteFolder) {
    await log.step(`Updating existing remote dataset: ${publication.remoteFolder.id}`)
    try {
      const res = await client.request({
        method: 'PATCH',
        url: `dataset/datasets/${publication.remoteFolder.id}/`,
        data: onegeoDataset
      })
      const finalDatasetId = res.data.id || publication.remoteFolder.id
      publication.remoteFolder = {
        id: String(finalDatasetId),
        title: res.data.display_name || onegeoDataset.display_name,
        url: res.data.detail_url || `${catalogConfig.url}/dataset/${publication.remoteFolder.id}`
      }
      await log.info('Update successful on OneGeo Suite')
    } catch (error: any) {
      if (error.response?.status === 404) {
        throw new Error(`The remote dataset ${publication.remoteFolder.id} no longer exists on OneGeo Suite.`)
      }
      throw new Error(`Error during update: ${error.message}`)
    }
  } else {
    await log.step('Creating a new dataset on OneGeo Suite')
    try {
      const res = await client.request({
        method: 'POST',
        url: 'dataset/datasets/',
        data: onegeoDataset
      })
      const finalDatasetId = res.data.id
      publication.remoteFolder = {
        id: String(finalDatasetId),
        title: res.data.display_name,
        url: res.data.detail_url || `${catalogConfig.url}/dataset/${res.data.id}`
      }
      await log.info(`New dataset created with ID: ${res.data.id}`)
      await addPageLink(client, finalDatasetId, dataset, catalogConfig, publicationSite, log)
      await addDownloadLink(client, finalDatasetId, dataset, catalogConfig, publicationSite, log)
    } catch (error: any) {
      throw new Error(`Error during creation on OneGeo Suite: ${error.response?.data ? JSON.stringify(error.response.data) : error.message}`)
    }
  }

  await log.info('Dataset publication completed successfully')
  return publication
}

const deleteDataset = async ({ catalogConfig, folderId, log }: DeletePublicationContext<OneGeoSuiteConfig>, client: OneGeoClient): Promise<void> => {
  try {
    await log.step(`Deleting dataset ${folderId}`)
    await client.request({
      method: 'DELETE',
      url: `dataset/datasets/${folderId}/`
    })
    await log.info(`Dataset ${folderId} deleted successfully`)
  } catch (e: any) {
    await log.error(`Error deleting dataset: ${e.message}`)
    if (![404, 410].includes(e.response?.status)) {
      throw new Error(`Error during deletion on ${catalogConfig.url}: ${e.message}`)
    }
    await log.warning(`Dataset ${folderId} does not exist or has already been deleted (code ${e.response?.status})`)
  }
}

const createOrUpdateResource = async ({ catalogConfig, dataset, publication, publicationSite, log }: PublishDatasetContext<OneGeoSuiteConfig, OneGeoCapabilities>, client: OneGeoClient): Promise<Publication> => {
  await log.step('Preparing the resource for publication on OneGeo Suite')
  let datasetIdString = publication.remoteFolder?.id

  if (!datasetIdString && publication.remoteResource?.id?.includes(':')) {
    datasetIdString = publication.remoteResource.id.split(':')[0]
  }

  if (!datasetIdString) {
    throw new Error('The parent dataset ID is required to publish a resource')
  }
  if (catalogConfig?.usergroup?.id === undefined) {
    throw new Error('An organization is required to publish a resource.')
  }

  datasetIdString = String(datasetIdString)
  const datasetId = parseInt(datasetIdString, 10)

  if (!publication.remoteFolder) {
    publication.remoteFolder = { id: datasetIdString }
  } else {
    publication.remoteFolder.id = datasetIdString
  }
  const usergroupId = catalogConfig.usergroup.id
  const exportUrl = microTemplate(publicationSite.datasetUrlTemplate || '', { id: dataset.id, slug: dataset.slug })
  const resourceSlug = dataset.id.replace(/[^a-zA-Z0-9_-]/g, '-').substring(0, 90) + '-res'

  try {
    const compositeId = publication.remoteResource?.id
    let actualResourceId: string | undefined

    if (compositeId) {
      const parts = compositeId.split(':')
      actualResourceId = parts.length === 2 ? parts[1] : compositeId
    }

    if (!actualResourceId) {
      await log.info('1. Creating the resource shell')
      const resResource = await client.request({
        method: 'POST',
        url: 'resource/resources/',
        data: {
          codename: resourceSlug,
          display_name: dataset.title,
          usergroup_id: usergroupId
        }
      })
      actualResourceId = String(resResource.data.id)

      await log.info(`2. Creating the link attached to resource ${actualResourceId}`)
      await client.request({
        method: 'POST',
        url: 'resource/href/',
        data: {
          href: exportUrl,
          resource_id: parseInt(actualResourceId, 10)
        }
      })

      await log.info('3. Linking the resource to the dataset')
      await client.request({
        method: 'POST',
        url: 'resource/resource-dataset/',
        data: {
          resource_id: parseInt(actualResourceId, 10),
          dataset_id: datasetId,
          publish: true,
          type: 1
        }
      })
    } else {
      await log.info(`Updating resource ${actualResourceId}`)
      await client.request({
        method: 'PATCH',
        url: `resource/resources/${actualResourceId}/`,
        data: {
          display_name: dataset.title
        }
      })
    }
    publication.remoteResource = {
      id: `${datasetIdString}:${actualResourceId}`,
      title: `${dataset.title} - Lien externe`,
      url: exportUrl
    }
    await log.info('Resource publication completed successfully')
    return publication
  } catch (error: any) {
    throw new Error(`Error creating the resource: ${error.response?.data ? JSON.stringify(error.response.data) : error.message}`)
  }
}

const deleteResource = async ({ resourceId, log }: DeletePublicationContext<OneGeoSuiteConfig>, client: OneGeoClient): Promise<void> => {
  try {
    if (!resourceId) {
      throw new Error('The resource ID is required for deletion')
    }
    const actualResourceId = resourceId.includes(':') ? resourceId.split(':')[1] : resourceId

    await log.step(`Deleting resource ${actualResourceId}`)
    await client.request({
      method: 'DELETE',
      url: `resource/resources/${actualResourceId}/`
    })
    await log.info(`Resource ${actualResourceId} deleted successfully`)
  } catch (e: any) {
    if (![404, 410].includes(e.response?.status)) {
      throw new Error(`Error deleting the resource: ${e.message}`)
    }
    await log.warning(`Resource ${resourceId} does not exist or has already been deleted`)
  }
}

const addDownloadLink = async (client: OneGeoClient, datasetId: number, dataset: any, catalogConfig: any, publicationSite: any, log: any) => {
  if (!dataset.originalFile) return

  await log.info('Adding direct download link...')
  const useSlug = !!(publicationSite.datasetUrlTemplate && publicationSite.datasetUrlTemplate.includes('slug'))
  const downloadUrl = `${publicationSite.url}/data-fair/api/v1/datasets/${useSlug ? dataset.slug : dataset.id}/raw`
  const fileExtension = dataset.originalFile.name.split('.').pop().toLowerCase()

  try {
    const resShell = await client.request({
      method: 'POST',
      url: 'resource/resources/',
      data: {
        codename: `${dataset.id}-dl`.substring(0, 100),
        display_name: `Télécharger les données (${fileExtension.toUpperCase()})`,
        usergroup_id: catalogConfig.usergroup.id
      }
    })
    const resourceId = resShell.data.id

    let dataformatId: number | null = null
    try {
      const formatRes = await client.request({
        method: 'GET',
        url: '/resource/data-format/',
        params: { search: fileExtension }
      })
      const formats = formatRes.data.results || formatRes.data
      if (formats && formats.length > 0) {
        const exactMatch = formats.find((f: any) =>
          f.codename && f.codename.toLowerCase() === fileExtension
        )
        if (exactMatch) {
          dataformatId = exactMatch.id
        } else {
          dataformatId = formats[0].id
        }
      }
    } catch (e) {
    }

    if (dataformatId) {
      await client.request({
        method: 'POST',
        url: 'resource/file-download/',
        data: {
          resource_id: resourceId,
          dataformat_id: dataformatId,
          href: downloadUrl
        }
      })
    } else {
      await log.warning(`Format '${fileExtension}' not found in OneGeo, using a generic link`)
      await client.request({
        method: 'POST',
        url: 'resource/href/',
        data: {
          href: downloadUrl,
          resource_id: resourceId
        }
      })
    }
    await client.request({
      method: 'POST',
      url: 'resource/resource-dataset/',
      data: {
        resource_id: resourceId,
        dataset_id: datasetId,
        publish: true,
        type: 2
      }
    })
  } catch (error: any) {
    await log.warning(`Unable to add download link: ${error.response?.data ? JSON.stringify(error.response.data) : error.message}`)
  }
}

const addPageLink = async (client: OneGeoClient, datasetId: number, dataset: any, catalogConfig: any, publicationSite: any, log: any) => {
  await log.info('Adding link to the dataset page...')
  const exportUrl = microTemplate(publicationSite.datasetUrlTemplate || '', { id: dataset.id, slug: dataset.slug })
  const resourceSlug = dataset.id.replace(/[^a-zA-Z0-9_-]/g, '-').substring(0, 90) + '-page'

  try {
    const resShell = await client.request({
      method: 'POST',
      url: 'resource/resources/',
      data: {
        codename: resourceSlug,
        display_name: 'Explorer les données',
        usergroup_id: catalogConfig.usergroup.id
      }
    })
    const resourceId = resShell.data.id
    await client.request({
      method: 'POST',
      url: 'resource/href/',
      data: {
        href: exportUrl,
        resource_id: resourceId
      }
    })
    await client.request({
      method: 'POST',
      url: 'resource/resource-dataset/',
      data: {
        resource_id: resourceId,
        dataset_id: datasetId,
        publish: true,
        type: 1
      }
    })
  } catch (error: any) {
    await log.warning(`Unable to add the view link: ${error.response?.data ? JSON.stringify(error.response.data) : error.message}`)
  }
}
