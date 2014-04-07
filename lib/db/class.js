var Promise = require('bluebird');

/**
 * The cached class items.
 * @type {Object|false}
 */
exports.cached = false;

/**
 * Retreive a list of classes from the database.
 *
 * @param  {Boolean} refresh Whether to refresh the list or not.
 * @promise {Object[]}       An array of class objects.
 */
exports.list = function (refresh) {
  if (!refresh && this.class.cached)
    return Promise.resolve(this.class.cached.items);

  return this.send('record-load', {
    cluster: 0,
    position: 1
  })
  .bind(this)
  .then(function (response) {
    var record = response.records[0];
    if (!record || !record.classes)
      return [];
    else
      return record.classes;
  })
  .then(this.class.cacheData)
  .then(function () {
    return this.class.cached.items;
  });
};

/**
 * Create a new class.
 *
 * @param  {String} name            The name of the class to create.
 * @param  {String} parentName      The name of the parent to extend, if any.
 * @param  {String|Integer} cluster The cluster name or id.
 * @promise {Object}                The created class object
 */
exports.create = function (name, parentName, cluster) {
  var query = 'CREATE CLASS ' + name;

  if (parentName) {
    query += ' EXTENDS ' + parentName;
  }

  if (cluster) {
    query += ' CLUSTER ' + cluster;
  }

  return this.query(query)
  .bind(this)
  .then(function () {
    return this.class.list(true);
  })
  .then(function (classes) {
    return this.class.get(name);
  });
};


/**
 * Delete a class.
 *
 * @param  {String} name The name of the class to delete.
 * @promise {Db}         The database instance.
 */
exports.delete = function (name, parentName, cluster) {
  return this.query('DROP CLASS ' + name)
  .bind(this)
  .then(function () {
    return this.class.list(true);
  })
  .then(function (classes) {
    return this;
  });
};


/**
 * Get a class by name.
 *
 * @param   {Integer|String} name The name of the class.
 * @param   {Boolean} refresh Whether to refresh the data, defaults to false.
 * @promise {Object}          The class object if it exists.
 */
exports.get = function (name, refresh) {
  if (!refresh && this.class.cached && this.class.cached.names[name]) {
    return Promise.resolve(this.class.cached.names[name]);
  }
  else if (!this.class.cached || refresh) {
    return this.class.list(refresh)
    .bind(this)
    .then(function () {
      return this.class.cached.names[name];
    });
  }
  else
    return Promise.resolve(undefined);
};

/**
 * Cache the given class data for fast lookup later.
 *
 * @param  {Object[]} classes The class objects to cache.
 * @return {Db}                The db instance.
 */
exports.cacheData = function (classes) {
  var total = classes.length,
      item, i;

  this.class.cached = {
    names: {},
    items: classes
  };

  for (i = 0; i < total; i++) {
    item = classes[i];
    this.class.cached.names[item.name] = item;
  }

  return this;
};