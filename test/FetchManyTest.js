require('mocha-sinon')

const os = require('os')
const path = require('path')
const crypto = require('crypto')

const chai = require('chai')
chai.use(require('chai-fs'))
const assert = chai.assert

const _ = require('lodash')
const rimraf = require('rimraf')
const touch = require('touch')
const logger = require('./fixtures/mockLogger')
const mockApi = require('./fixtures/mockApiObjects')

const FetchMany = require('../src/FetchMany')
function buildTestFetchMany(tableNames) {
  const tmpDir = path.join(os.tmpdir(), crypto.randomBytes(12).toString('hex'))
  const config = {
    tmpDir: tmpDir,
    saveLocation: path.join(tmpDir, 'dataFiles'),
    unpackLocation: path.join(tmpDir, 'unpackedFiles'),
    apiUrl: 'https://mockApi/api',
    key: 'fakeKey',
    secret: 'fakeSecret'
  }
  return {fetchMany: new FetchMany({tables: tableNames || 'account_dim', concurrency: 5}, config, logger), config}
}

function cleanupFetchMany(sync, config, cb) {
  rimraf(config.tmpDir, cb)
}

describe('FetchManyTest', function() {
  describe('fetchTable', () => {
    it('should be able to fetch a single table', function(done) {
      const tableName = 'user_dim'
      var {fetchMany, config} = buildTestFetchMany(tableName)
      // Stub the API and File Downloader of all the fetch instances
      fetchMany.fetchList.forEach((fetchInstance) => {
        var apiStub = this.sinon.stub(fetchInstance.api, 'getFilesForTable', (opts, cb) => {
          cb(null, mockApi.buildDumpHistory({table: tableName}))
        })
        fetchInstance.downloadStub = this.sinon.stub(fetchInstance.fileDownloader, 'downloadToFile', (filename, opts, savePath, cb) => {
          touch(savePath, cb)
        })
      })

      fetchMany.fetchTable(fetchMany.fetchList[0], (err, showComplete) => {
        assert.ifError(err)
        assert(fetchMany.fetchList[0].downloadStub.callCount, 2)
        assert.isFile(path.join(config.saveLocation, tableName, '0-filename-1.tar.gz'))
        cleanupFetchMany(fetchMany, config, done)
      })
    })
  })
  describe('run', () => {
    it('should be able fetch multiple tables', function(done) {
      const tableNames = 'user_dim,account_dim,partial_dim'
      var {fetchMany, config} = buildTestFetchMany(tableNames)
      // Stub the API and File Downloader of all the fetch instances
      fetchMany.fetchList.forEach((fetchInstance) => {
        var apiStub = this.sinon.stub(fetchInstance.api, 'getFilesForTable', (opts, cb) => {
          cb(null, mockApi.buildDumpHistory({table: fetchInstance.table}))
        })
        fetchInstance.downloadStub = this.sinon.stub(fetchInstance.fileDownloader, 'downloadToFile', (filename, opts, savePath, cb) => {
          touch(savePath, cb)
        })
      })

      fetchMany.run((err, showComplete) => {
        assert.ifError(err)
        fetchMany.fetchList.forEach((fetchInstance) => {
          assert.isFile(path.join(config.saveLocation, fetchInstance.table, '0-filename-1.tar.gz'))
        })
        cleanupFetchMany(fetchMany, config, done)
      })
    }).timeout(10000)
  })
})
