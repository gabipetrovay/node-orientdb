var Promise = require('bluebird'),
    RID = require('../recordid'),
    errors = require('../errors');

/**
 * Insert the given record into the database.
 *
 * @param  {Object} record  The record to insert.
 * @promise {Object}        The inserted record.
 */
exports.create = function (record) {
  var className = record['@class'] || '',
      options = record['@options'] || {},
      rid, promise;

  if (record['@rid']) {
    promise = Promise.resolve(record['@rid']);
  }
  else if (className !== '') {
    promise = this.cluster.getByName(className);
  }

  return promise
  .bind(this)
  .then(function (cluster) {
    if (!cluster) {
      return Promise.reject(new errors.Operation('Cannot create record -  cluster ID and/or class is invalid.'));
    }
    else if (cluster instanceof RID) {
      rid = cluster;
    }
    else {
      rid = new RID({
        cluster: cluster.id || cluster,
        position: -1
      });
    }
    return this.send('record-create', {
      segment: options.segment != null ? +options.segment : -1,
      cluster: rid.cluster,
      record: record
    })
  })
  .then(function (results) {
    rid.position = results.position;
    record['@rid'] = rid;
    record['@version'] = results.version;
    return record;
  });
};

/**
 * Read the given record.
 *
 * @param  {Object} record The record to load.
 * @promise {Object}       The loaded record.
 */
exports.get = function (record) {
  if (Array.isArray(record)) {
    return Promise.all(record.map(this.record.read, this));
  }
  var extracted = extractRecordId(record),
      rid = extracted[0],
      record = extracted[1],
      options = record['@options'] || {};

  if (!rid) {
    return Promise.reject(new errors.Operation('Cannot read - no record id specified'));
  }

  var options = record['@options'] || {};

  return this.send('record-load', {
    cluster: rid.cluster,
    position: rid.position,
    fetchPlan: options.fetchPlan || '',
    tombstones: options.tombstones || false
  })
  .bind(this)
  .then(function (response) {
    if (response.records.length === 1)
      return response.records[0];
    else
      return response.records;
  });
};

/**
 * Read the metadata for the given record.
 *
 * @param  {Object} record The record to load.
 * @promise {Object}       The record object with loaded meta data.
 */
exports.meta = function (record) {
  if (Array.isArray(record)) {
    return Promise.all(record.map(this.record.read, this));
  }
  var extracted = extractRecordId(record),
      rid = extracted[0],
      record = extracted[1],
      options = record['@options'] || {};

  if (!rid) {
    return Promise.reject(new errors.Operation('Cannot read - no record id specified'));
  }

  var options = record['@options'] || {};

  return this.send('record-metadata', {
    cluster: rid.cluster,
    position: rid.position
  })
  .bind(this)
  .then(function (response) {
    record['@rid'] = rid;
    record['@version'] = response.version;
    return record;
  });
};

/**
 * Update the given record.
 *
 * @param  {Object} record The record to update.
 * @promise {Object}       The updated record.
 */
exports.update = function (record) {
  var extracted = extractRecordId(record),
      rid = extracted[0],
      record = extracted[1],
      options = record['@options'] || {},
      promise, data;

  if (!rid) {
    return Promise.reject(new errors.Operation('Cannot update record -  record ID is not specified or invalid.'));
  }

  record['@type']  = 'd';

  data = {
    cluster: rid.cluster,
    position: rid.position,
    mode: options.mode || 0
  };

  if (options.preserve && rid) {
    promise = this.record.get(rid)
    .then(function (found) {
      var keys = Object.keys(record),
          total = keys.length,
          key, i;
      for (i = 0; i < total; i++) {
        key = keys[i];
        found[key] = record[key];
      }
      return found;
    });
  }
  else
    promise = Promise.resolve(record);

  return promise
  .bind(this)
  .then(function (record) {
    data.record = record;
    return this.send('record-update', data)
    .then(function (results) {
      delete record['@options'];
      record['@version'] = (results ? results.version : 0) || 0;
      return record;
    });
  });
};

/**
 * Delete the given record.
 *
 * @param   {String|RID|Object} record  The record or record id to delete.
 * @promise {Object}                    The deleted record object.
 */
exports.delete = function (record) {
  if (!record) {
    return Promise.reject(new errors.Operation('Cannot delete - no record specified'));
  }
  var extracted = extractRecordId(record),
      rid = extracted[0],
      record = extracted[1],
      options = record['@options'] || {};

  if (!rid) {
    return Promise.reject(new errors.Operation('Cannot delete - no record id specified'));
  }

  return this.send('record-delete', {
    cluster: rid.cluster,
    position: rid.position,
    version: record['@version'] != null ? +record['@version'] : -1,
    mode: options.mode || 0
  })
  .then(function (response) {
    return record;
  });
};

/**
 * Extract the record id and record from the given argument.
 *
 * @param  {String|RID|Object} record The record.
 * @return {[RID, Object]}            The record id and object.
 */
function extractRecordId (record) {
  var rid = false;
  if (typeof record === 'string') {
    rid = RID.parse(record);
    record = {
      '@rid': rid
    };
  }
  else if (record instanceof RID) {
    rid = record;
    record = {
      '@rid': rid
    };
  }
  else if (record['@rid']) {
    record['@rid'] = rid = RID.parse(record['@rid']);
  }
  return [rid, record];
}