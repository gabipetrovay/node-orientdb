exports['connect'] = require('./connect');
exports['db-open'] = require('./db-open');
exports['db-create'] = require('./db-create');
exports['db-exists'] = require('./db-exists');
exports['db-delete'] = require('./db-delete');
exports['db-size'] = require('./db-size');
exports['db-countrecords'] = require('./db-countrecords');
exports['db-reload'] = require('./db-reload');
exports['db-list'] = require('./db-list');
exports['db-close'] = require('./db-close');


exports['datacluster-add'] = require('./datacluster-add');
exports['datacluster-count'] = require('./datacluster-count');
exports['datacluster-datarange'] = require('./datacluster-datarange');
exports['datacluster-drop'] = require('./datacluster-drop');

exports['datasegment-add'] = require('./datasegment-add');
exports['datasegment-drop'] = require('./datasegment-drop');


exports['record-create'] = require('./record-create');
exports['record-load'] = require('./record-load');
exports['record-metadata'] = require('./record-metadata');
exports['record-update'] = require('./record-update');
exports['record-delete'] = require('./record-delete');
exports['record-clean-out'] = require('./record-clean-out');

exports['command'] = require('./command');

exports['config-list'] = require('./config-list');
exports['config-get'] = require('./config-get');
exports['config-set'] = require('./config-set');