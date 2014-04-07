var Operation = require('../operation'),
    constants = require('../constants'),
    RID = require('../../recordid'),
    serializer = require('../serializer');

module.exports = Operation.extend({
  id: 'REQUEST_RECORD_UPDATE',
  opCode: 32,
  writer: function () {
    var rid, cluster, position, version;
    if (this.data.record['@rid']) {
      rid = RID.parse(this.data.record['@rid']);
      cluster = this.data.cluster || rid.cluster;
      position = this.data.position || rid.position;
    }
    else {
      cluster = this.data.cluster;
      position = this.data.position;
    }
    if (this.data.version != null) {
      version = this.data.version;
    }
    else if (this.data.record['@version'] != null) {
      version = this.data.record['@version'];
    }
    else {
      version = -1;
    }
    this
    .writeByte(this.opCode)
    .writeInt(this.data.sessionId)
    .writeShort(cluster)
    .writeLong(position)
    .writeBytes(serializer.encodeRecordData(this.data.record))
    .writeInt(version)
    .writeByte(constants.RECORD_TYPES[this.data.type || 'd'])
    .writeByte(this.data.mode || 0);
  },
  reader: function () {
    this
    .readStatus('status')
    .readInt('version');
  }
});