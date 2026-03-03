import type CatalogPlugin from '@data-fair/types-catalogs'
import { strict as assert } from 'node:assert'
import { it, describe, before, beforeEach } from 'node:test'
import fs from 'fs-extra'
import { logFunctions } from './test-utils.ts'

// Import plugin and use default type like it's done in Catalogs
import plugin from '../index.ts'
const catalogPlugin: CatalogPlugin = plugin as CatalogPlugin

// Config pour les tests de lecture (list, getResource)
const catalogConfig = { url: 'https://www.datasud.fr/fr/' }
const tmpDir = './data/test/downloads'
const getResourceParams = {
  catalogConfig,
  secrets: {},
  importConfig: {},
  update: { metadata: true, schema: true },
  tmpDir,
  log: logFunctions
}

describe('catalog-OneGeoSuite', () => {
  it('should list resources from root', async () => {
    const res = await catalogPlugin.list({
      catalogConfig,
      secrets: {},
      params: {}
    })
    assert.equal(res.results[0].type, 'resource', 'Expected folders in the root folder')
    assert.equal(res.path.length, 0, 'Expected no path for root folder')
  })

  it('should list resources from root with pagination', async () => {
    const res = await catalogPlugin.list({
      catalogConfig,
      secrets: {},
      params: { size: 20, page: 2, q: '*' }
    })
    assert.ok(res.results.length > 0, 'Expected pagination')
    assert.ok(res.results.length = 20, 'Expected pagination')
    assert.equal(res.results[0].type, 'resource', 'Expected resources in the root folder')
    assert.equal(res.path.length, 0, 'Expected no path for root folder')
  })

  describe('should download a resource', async () => {
    before(async () => await fs.ensureDir(tmpDir))
    beforeEach(async () => await fs.emptyDir(tmpDir))

    await it('with correct params', async () => {
      const resources = await catalogPlugin.list({
        catalogConfig,
        secrets: {},
        params: {}
      })
      const resourceId = resources.results[0].id

      const resource = await catalogPlugin.getResource({
        ...getResourceParams,
        resourceId
      })

      assert.ok(resource, 'The resource should exist')
      assert.ok(resource.filePath, 'Download URL should not be undefined')

      const fileExists = await fs.pathExists(resource.filePath)
      assert.ok(fileExists, 'The downloaded file should exist')
    })
  })

  describe('publication to local OneGeoSuite', async () => {
    const pubCatalogConfig = {
      url: 'http://localhost:8089/fr/',
      usergroup: {
        id: 10,
        display_name: 'Organisation Locale de test'
      }
    }
    // Identifiants locaux
    const pubSecrets = {
      username: 'admin',
      password: 'qsdfghjklm'
    }

    // Mock d'un jeu de données Data Fair
    const mockDataset = {
      id: 'test-dataset-003',
      title: 'Mon jeu de données de test unitaire',
      description: 'Ceci est un test automatisé depuis Data Fair',
      public: true,
      slug: 'test-api-dataset'
    }

    // Mock de l'URL du portail Data Fair
    const publicationSite = {
      url: 'http://localhost:8080',
      datasetUrlTemplate: 'http://localhost:8080/datasets/{id}'
    }

    let publishedFolderId: string
    let publishedResourceId: string

    it('should publish a dataset (createFolder)', async () => {
      const res = await catalogPlugin.publishDataset({
        catalogConfig: pubCatalogConfig,
        secrets: pubSecrets,
        dataset: mockDataset as any,
        publication: { action: 'createFolderInRoot' } as any,
        publicationSite: publicationSite as any,
        log: logFunctions
      })
      logFunctions.info('Publication response:', res)
      assert.ok(res.remoteFolder?.id, 'Un ID distant (remoteFolder) aurait dû être retourné')
      publishedFolderId = res.remoteFolder!.id
      console.log('=> Dataset créé avec l\'ID :', publishedFolderId)
    })

    it('should publish a resource (createResource)', async () => {
      assert.ok(publishedFolderId, 'L\'ID du dataset est requis pour publier la ressource')

      const res = await catalogPlugin.publishDataset({
        catalogConfig: pubCatalogConfig,
        secrets: pubSecrets,
        dataset: mockDataset as any,
        publication: {
          action: 'createResource',
          remoteFolder: { id: publishedFolderId } // On indique le parent
        } as any,
        publicationSite: publicationSite as any,
        log: logFunctions
      })

      assert.ok(res.remoteResource?.id, 'Un ID distant (remoteResource) aurait dû être retourné')
      publishedResourceId = res.remoteResource!.id
      console.log('=> Ressource créée avec l\'ID :', publishedResourceId)
    })
    it('should update the dataset (replaceFolder)', async () => {
      assert.ok(publishedFolderId, 'L\'ID du dataset est requis pour la mise à jour')
      const modifiedDataset = {
        ...mockDataset,
        title: 'Mon jeu de données de test unitaire (Modifié)',
        description: 'Description mise à jour lors du test de replaceFolder'
      }

      const res = await catalogPlugin.publishDataset({
        catalogConfig: pubCatalogConfig,
        secrets: pubSecrets,
        dataset: modifiedDataset as any,
        publication: {
          action: 'replaceFolder',
          remoteFolder: { id: publishedFolderId }
        } as any,
        publicationSite: publicationSite as any,
        log: logFunctions
      })
      assert.ok(res.remoteFolder?.id, 'L\'ID distant (remoteFolder) aurait dû être conservé/retourné')
      assert.equal(res.remoteFolder!.id, publishedFolderId, 'L\'ID du dossier ne devrait pas changer lors d\'une mise à jour')
      console.log('=> Dataset mis à jour avec l\'ID :', res.remoteFolder!.id)
    })

    it('should update the resource (replaceResource)', async () => {
      assert.ok(publishedFolderId, 'L\'ID du dataset est requis pour publier la ressource')
      assert.ok(publishedResourceId, 'L\'ID de la ressource est requis pour la mise à jour')

      const modifiedDataset = {
        ...mockDataset,
        title: 'Mon jeu de données de test unitaire (Modifié)'
      }

      const res = await catalogPlugin.publishDataset({
        catalogConfig: pubCatalogConfig,
        secrets: pubSecrets,
        dataset: modifiedDataset as any,
        publication: {
          action: 'replaceResource',
          remoteFolder: { id: publishedFolderId },
          remoteResource: { id: publishedResourceId }
        } as any,
        publicationSite: publicationSite as any,
        log: logFunctions
      })

      assert.ok(res.remoteResource?.id, 'Un ID distant (remoteResource) aurait dû être conservé/retourné')
      assert.equal(res.remoteResource!.id, publishedResourceId, 'L\'ID de la ressource ne devrait pas changer lors d\'une mise à jour')
      console.log('=> Ressource mise à jour avec l\'ID :', res.remoteResource!.id)
    })

    it('should delete the resource', async () => {
      assert.ok(publishedResourceId, 'L\'ID de la ressource est requis')
      await catalogPlugin.deletePublication({
        catalogConfig: pubCatalogConfig,
        secrets: pubSecrets,
        folderId: publishedFolderId,
        resourceId: publishedResourceId,
        log: logFunctions
      })
      assert.ok(true)
    })

    it('should delete the dataset', async () => {
      assert.ok(publishedFolderId, 'L\'ID du dataset est requis')
      await catalogPlugin.deletePublication({
        catalogConfig: pubCatalogConfig,
        secrets: pubSecrets,
        folderId: publishedFolderId,
        log: logFunctions
      })
      assert.ok(true)
    })
  })
})
