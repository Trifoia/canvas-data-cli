var Fetch = require('./Fetch')
var lodash = require('lodash')
var async = require('async')
class FetchMany {
  constructor(opts, config, logger) {
    this.opts = opts
    this.logger = logger
    
    // Process the list of options and create as many Fetch instances as we need
    this.tableList = opts.tables.split(',')
    this.fetchList = this.tableList.map((table) => {
      var singleFetchOpts = lodash.clone(this.opts)
      singleFetchOpts.table = table
      return new Fetch(singleFetchOpts, config, logger)
    })
  }
  fetchTable(fetchInstance, cb) {
    this.logger.info(`Fetching table: ${fetchInstance.table}`)
    fetchInstance.run((err, showComplete) => {
      if (err) return cb(err)

      this.logger.info(`Fetch complete for table: ${fetchInstance.table}`)
      return cb(null, showComplete)
    })
  }
  run(cb) {
    async.mapLimit(this.fetchList, this.opts.concurrency, this.fetchTable.bind(this), (err) => {
      if (err) return cb(err)
      cb()
    })
  }
}
module.exports = FetchMany
