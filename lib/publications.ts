import type { CatalogPlugin, Publication, PublishDatasetContext, DeletePublicationContext } from '@data-fair/types-catalogs'
import type { OneGeoSuiteConfig } from '#types'
import type { OneGeoCapabilities } from './capabilities.ts'
import type { OneGeoClient } from './onegeo-client.ts'

import { microTemplate } from '@data-fair/lib-utils/micro-template.js'
import { createOneGeoClient } from './onegeo-client.ts'

export const publishDataset = async (context: PublishDatasetContext<OneGeoSuiteConfig, OneGeoCapabilities>): ReturnType<CatalogPlugin['publishDataset']> => {
  if (!context.secrets?.username || !context.secrets?.password) {
    throw new Error('Un nom d\'utilisateur et un mot de passe sont requis pour publier sur OneGeo Suite')
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
    throw new Error('Un nom d\'utilisateur et un mot de passe sont requis pour supprimer une publication sur OneGeo Suite')
  }

  const oneGeoClient = createOneGeoClient(context.catalogConfig.url, context.secrets)

  if (context.resourceId) return await deleteResource(context, oneGeoClient)
  else await deleteDataset(context, oneGeoClient)
}

const createOrUpdateDataset = async ({ catalogConfig, dataset, publication, publicationSite, log }: PublishDatasetContext<OneGeoSuiteConfig, OneGeoCapabilities>, client: OneGeoClient): Promise<Publication> => {
  await log.step('Préparation du jeu de données pour publication/mise à jour sur OneGeo Suite')

  await log.step('Construction des métadonnées OneGeo Suite')
  const slug = dataset.id.replace(/[^a-zA-Z0-9_-]/g, '-').substring(0, 100)
  if (catalogConfig?.usergroup?.id === undefined) {
    await log.warning('Aucune organisation spécifiée dans la configuration du catalogue.')
    throw new Error('L\'organisation est requise pour publier sur OneGeo Suite. Veuillez ajouter une organisation dans la configuration du catalogue.')
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
    await log.step(`Mise à jour du jeu de données distant existant : ${publication.remoteFolder.id}`)
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
      await log.info('Mise à jour réussie sur OneGeo Suite')
    } catch (error: any) {
      if (error.response?.status === 404) {
        throw new Error(`Le jeu de données distant ${publication.remoteFolder.id} n'existe plus sur OneGeo Suite.`)
      }
      throw new Error(`Erreur lors de la mise à jour: ${error.message}`)
    }
  } else {
    await log.step('Création d\'un nouveau jeu de données sur OneGeo Suite')
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
      await log.info(`Nouveau jeu de données créé avec l'ID : ${res.data.id}`)

      await addDownloadLink(client, finalDatasetId, dataset, catalogConfig, publicationSite, log)
    } catch (error: any) {
      throw new Error(`Erreur lors de la création sur OneGeo Suite: ${error.response?.data ? JSON.stringify(error.response.data) : error.message}`)
    }
  }

  await log.info('Publication du Dataset terminée avec succès')
  return publication
}

const deleteDataset = async ({ catalogConfig, folderId, log }: DeletePublicationContext<OneGeoSuiteConfig>, client: OneGeoClient): Promise<void> => {
  try {
    await log.step(`Suppression du jeu de données ${folderId}`)
    await client.request({
      method: 'DELETE',
      url: `dataset/datasets/${folderId}/`
    })
    await log.info(`Jeu de données ${folderId} supprimé avec succès`)
  } catch (e: any) {
    await log.error(`Erreur lors de la suppression du jeu de données : ${e.message}`)
    if (![404, 410].includes(e.response?.status)) {
      throw new Error(`Erreur lors de la suppression sur ${catalogConfig.url} : ${e.message}`)
    }
    await log.warning(`Le jeu de données ${folderId} n'existe pas ou a déjà été supprimé (code ${e.response?.status})`)
  }
}

const createOrUpdateResource = async ({ catalogConfig, dataset, publication, publicationSite, log }: PublishDatasetContext<OneGeoSuiteConfig, OneGeoCapabilities>, client: OneGeoClient): Promise<Publication> => {
  await log.step('Préparation de la ressource pour publication sur OneGeo Suite')
  if (!publication.remoteFolder || !publication.remoteFolder.id) {
    throw new Error('L\'ID du jeu de données parent est requis pour publier une ressource')
  }
  if (catalogConfig?.usergroup?.id === undefined) {
    throw new Error('L\'organisation est requise pour publier une ressource.')
  }
  publication.remoteFolder.id = String(publication.remoteFolder.id).split(':').pop() || String(publication.remoteFolder.id)
  const datasetIdString = publication.remoteFolder.id
  const datasetId = parseInt(publication.remoteFolder.id, 10)
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
      await log.info('1. Création de la coquille Ressource')
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

      await log.info(`2. Création du lien attaché à la ressource ${actualResourceId}`)
      await client.request({
        method: 'POST',
        url: 'resource/href/',
        data: {
          href: exportUrl,
          resource_id: parseInt(actualResourceId, 10)
        }
      })

      await log.info('3. Liaison de la ressource au jeu de données')
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
      await log.info(`Mise à jour de la ressource ${actualResourceId}`)
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
    await log.info('Publication de la ressource terminée avec succès')
    return publication
  } catch (error: any) {
    throw new Error(`Erreur lors de la création de la ressource: ${error.response?.data ? JSON.stringify(error.response.data) : error.message}`)
  }
}

const deleteResource = async ({ resourceId, log }: DeletePublicationContext<OneGeoSuiteConfig>, client: OneGeoClient): Promise<void> => {
  try {
    if (!resourceId) {
      throw new Error('L\'ID de la ressource est requis pour la suppression')
    }

    // Extraction de l'ID réel si Data Fair envoie un ID composite
    const actualResourceId = resourceId.includes(':') ? resourceId.split(':')[1] : resourceId

    await log.step(`Suppression de la ressource ${actualResourceId}`)
    await client.request({
      method: 'DELETE',
      url: `resource/resources/${actualResourceId}/`
    })
    await log.info(`Ressource ${actualResourceId} supprimée avec succès`)
  } catch (e: any) {
    if (![404, 410].includes(e.response?.status)) {
      throw new Error(`Erreur lors de la suppression de la ressource : ${e.message}`)
    }
    await log.warning(`La ressource ${resourceId} n'existe pas ou a déjà été supprimée`)
  }
}

const addDownloadLink = async (client: OneGeoClient, datasetId: number, dataset: any, catalogConfig: any, publicationSite: any, log: any) => {
  if (!dataset.originalFile) return

  await log.info('Ajout du lien de téléchargement direct...')
  const useSlug = !!(publicationSite.datasetUrlTemplate && publicationSite.datasetUrlTemplate.includes('slug'))
  const downloadUrl = `${publicationSite.url}/data-fair/api/v1/datasets/${useSlug ? dataset.slug : dataset.id}/raw`
  const fileFormat = dataset.originalFile.name.split('.').pop()

  try {
    const resShell = await client.request({
      method: 'POST',
      url: 'resource/resources/',
      data: {
        codename: `${dataset.id}-dl`.substring(0, 100),
        display_name: `Télécharger les données (${fileFormat.toUpperCase()})`,
        usergroup_id: catalogConfig.usergroup.id
      }
    })
    const resourceId = resShell.data.id

    await client.request({
      method: 'POST',
      url: 'resource/href/',
      data: {
        href: downloadUrl,
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
        type: 2
      }
    })
    await log.info('Lien de téléchargement ajouté avec succès')
  } catch (error: any) {
    await log.warning(`Impossible d'ajouter le lien de téléchargement : ${error.message}`)
  }
}
