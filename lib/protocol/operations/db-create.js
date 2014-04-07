var Operation = require('../operation'),
    constants = require('../constants');

module.exports = Operation.extend({
  id: 'REQUEST_DB_CREATE',
  opCode: 4,
  writer: function () {
    this
    .writeByte(this.opCode)
    .writeInt(this.data.sessionId)
    .writeString(this.data.name)
    .writeString(this.data.type || 'graph')
    .writeString(this.data.storage || 'local');
  },
  reader: function () {
    this.readStatus('status');
  }
});