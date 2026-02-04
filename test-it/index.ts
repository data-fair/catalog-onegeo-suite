import type CatalogPlugin from '@data-fair/types-catalogs'
import { strict as assert } from 'node:assert'
import { it, describe, before, beforeEach } from 'node:test'
import fs from 'fs-extra'
import { logFunctions } from './test-utils.ts'
import dotenv from 'dotenv'

// Import plugin and use default type like it's done in Catalogs
import plugin from '../index.ts'
const catalogPlugin: CatalogPlugin = plugin as CatalogPlugin

dotenv.config()

const catalogConfig = {
  url: process.env.ONEGEO_SUITE_URL,
}

const tmpDir = './data/test/downloads'

const getResourceParams = {
  catalogConfig,
  secrets: {},
  resourceId: '',
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
      params: { size: 20, page: 2 }
    })

    assert.ok(res.results.length <= 20, 'Expected pagination')

    assert.equal(res.results[0].type, 'resource', 'Expected resourecs in the root folder')

    assert.equal(res.path.length, 0, 'Expected no path for root folder')
  })

  describe('should download a resource', async () => {
    // Ensure the temporary directory exists once for all tests
    before(async () => await fs.ensureDir(tmpDir))

    // Clear the temporary directory before each test
    beforeEach(async () => await fs.emptyDir(tmpDir))

    it('with correct params', async () => {
      const datasetId = (await catalogPlugin.list({
        catalogConfig,
        secrets: {},
        params: {}
      })).results[0].id
      const resourceId = (await catalogPlugin.list({
        catalogConfig,
        secrets: {},
        params: { currentFolderId: datasetId }
      })).results[0].id

      const resource = await catalogPlugin.getResource({
        ...getResourceParams,
        resourceId
      })

      assert.ok(resource, 'The resource should exist')

      assert.equal(resource.id, resourceId, 'Resource ID should match')

      assert.ok(resource.filePath, 'Download URL should not be undefined')

      // Check if the file exists
      const fileExists = await fs.pathExists(resource.filePath)
      assert.ok(fileExists, 'The downloaded file should exist')
    })
  })
})
