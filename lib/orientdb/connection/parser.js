/*------------------------------------------------------------------------------
  (public) readByte

  + buf
  + offset
  - number

  Read a byte as unsigned integer from the buffer at the given offset.
------------------------------------------------------------------------------*/
exports.readByte = function(buf, offset) {
	return buf.readUInt8(offset);
};

/*------------------------------------------------------------------------------
  (public) readBytes

  + buf
  + offset
  - number

  Read bytes from the buffer at the given offset.
------------------------------------------------------------------------------*/
exports.readBytes = function(buf, offset) {
  var length = exports.readInt(buf, offset);
  var new_offset = offset + 4;
  var bytes = new Buffer(length);
  buf.copy(bytes, 0, new_offset, length + new_offset);
  return bytes;
};

/*------------------------------------------------------------------------------
  (public) readShort

  + buf
  - number

  Read a 2-byte signed integer from the buffer at the given offset.
------------------------------------------------------------------------------*/
exports.readShort = function(buf, offset) {
	return buf.readInt16BE(offset);
};

/*------------------------------------------------------------------------------
  (public) readInt

  + buf
  - number

  Read a 4-byte signed integer from the buffer at the given offset.
------------------------------------------------------------------------------*/
exports.readInt = function(buf, offset) {	
	return buf.readInt32BE(offset);
};

/*------------------------------------------------------------------------------
  (public) readLong

  + buf
  - number

  Read a 8-byte signed integer from the buffer at the given offset.
------------------------------------------------------------------------------*/
exports.readLong = function(buf, offset) {
	return buf.readInt32BE(offset) * 4294967296 + buf.readUInt32BE(offset + 4);
};

/*------------------------------------------------------------------------------
  (public) readString
  
  + buf
  - string
  
  Read a string from the buffer at the given offset.
------------------------------------------------------------------------------*/
exports.readString = function(buf, offset) {
    var length = this.readInt(buf, offset);
    return buf.toString("utf8", offset + 4, offset + 4 + length);
};

/*------------------------------------------------------------------------------
  (public) readRecord
  
  + buf
  - record
  
  Read a record from the buffer at the given offset.
------------------------------------------------------------------------------*/
exports.readRecord = function(buf, offset) {

    var record = {};

    // record class
    record.class = this.readShort(buf, offset);
    offset += 2;

    if (record.class == -2) {
        // no record
        return null;;
    } else if (record.class == -3) {
        // rid
        // TODO
        throw new Error("Not implemented!")
    } else if (record.class == -1) {
        // no class id
        // TODO
        throw new Error("And what am I suposed to do here?");
    } else if (record.class > -1) {
        // valid

        // record type ('d' or 'b')
        record.type = String.fromCharCode(this.readByte(buf, offset));
        offset += 1;

        // cluster ID
        var clusterId = this.readShort(buf, offset);
        offset += 2;

        if (clusterId != -1) {
            // cluster position
            var clusterPosition = this.readLong(buf, offset);
            offset += 8;

            record.rid = "#" + clusterId + ":" + clusterPosition;
        } else {
            // jump over the cluster position
            offset += 8;
        }

        // record version
        record.version = this.readInt(buf, offset);
        offset += 4;

        // serialized record
        record.content = this.readString(buf, offset);
        offset += record.content.length + 4;

        // TODO improve
        // internal field to get the length outside this function for the next record
        record._end = offset;

    } else {
        throw new Error("Unknown record class id: " + record.class);
    }

    return record;
};

/*------------------------------------------------------------------------------
  (public) readCollection
  
  + buf
  - collection
  
  Read a collection from the buffer ar the given offset.
------------------------------------------------------------------------------*/
module.exports.readCollection = function(buf, offset) {

    var collection = [];

    // collection size
    var length = this.readInt(buf, offset);
    offset += 4;

    for (var i = 0; i < length; i++) {

        var record = this.readRecord(buf, offset);

        // do not add null records
        if (record) {
            // TODO improve
            offset = record._end;
            delete record._end;
            collection.push(record);
        } else {
            // null records consume one short with the status
            offset += 2;
        }
    }

    return collection;
};

/*------------------------------------------------------------------------------
  (public) writeByte
  
  + data
  + useBuffer (optional) - when returned value should be a buffer
  - bytes or buffer
  
  Parse data to 4 bytes which represents integer value.
------------------------------------------------------------------------------*/
module.exports.writeByte = function(data, useBuffer) {
	var byte = [data];
	
	if(useBuffer) {
		return new Buffer(byte);
	} else {
		return byte;
	}
};

/*------------------------------------------------------------------------------
  (public) writeInt
  
  + data
  - bytes or buffer
  
  Parse data to 4 bytes which represents integer value.
------------------------------------------------------------------------------*/
module.exports.writeInt = function(data) {
    var buf = new Buffer(4);
    buf.writeInt32BE(data, 0);
    return buf;
};

/*------------------------------------------------------------------------------
  (public) writeLong
  
  + data
  - bytes or buffer
  
  Parse data to 4 bytes which represents integer value.
------------------------------------------------------------------------------*/
module.exports.writeLong = function(data) {
    var buf = new Buffer(8);
    buf.fill(0);
    
    var high = data / 4294967296;
    high = high > 0 ? Math.floor(high) : Math.ceil(high);
    var low = data % 4294967296;
    buf.writeInt32BE(high, 0);
    buf.writeInt32BE(low, 4);
    return buf;
};

/*------------------------------------------------------------------------------
  (public) writeShort
  
  + data
  - bytes or buffer
  
  Parse data to 2 bytes which represents short value.
------------------------------------------------------------------------------*/
module.exports.writeShort = function(data) {
    var buf = new Buffer(2);
    buf.writeInt16BE(data, 0);
    return buf;
};

/*------------------------------------------------------------------------------
  (public) writeBytes
  
  + data
  - buffer
  
  Write byte data to buffer.
------------------------------------------------------------------------------*/
module.exports.writeBytes = function(data) {
    var length = data.length;
    var buf = new Buffer(4 + length);
    buf.writeInt32BE(length, 0);
    data.copy(buf, 4);
    return buf;
};

/*------------------------------------------------------------------------------
  (public) writeString
  
  + data
  - buffer
  
  Parse string data to buffer with UTF-8 encoding.
------------------------------------------------------------------------------*/
module.exports.writeString = function(data) {
    var length = data.length;
    var buf = new Buffer(4 + length);
    buf.writeInt32BE(length, 0);
    buf.write(data, 4)
    return buf;
};

