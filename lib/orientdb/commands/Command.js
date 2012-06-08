var util = require("util"),
    base = require("./CommandBase"),
    parser = require("../connection/parser"),
    OperationTypes = require("./operation_types"),

    command = function() {
        base.call(this);

        this.result = {};
        this.error = null;

        this.steps.push(readPayloadStatus);
        this.steps.push(readPayloadString);
        this.steps.push(readPayloadCollection);
        this.steps.push(readPayloadRecord);
    },

    markers = {
        readPayloadString: 3,
        readPayloadCollection: 4,
        readPayloadRecord: 5
    };

util.inherits(command, base);

module.exports = command;

command.operation = OperationTypes.COMMAND;

command.write = function(socket, sessionId, data, callback) {

    // operation type
    socket.write(parser.writeByte(command.operation, true));

    // invoke callback imidiately when the operation is sent to the server
    callback();

    // session ID
    socket.write(parser.writeInt(sessionId, true));

    // mode
    socket.write(parser.writeByte(data.mode.charCodeAt(), true));

    // serialized command
    var buffers = [];

    // TODO query class name
    buffers.push(parser.writeString("com.orientechnologies.orient.core.sql.OCommandSQL"));

    // query text
    buffers.push(parser.writeString(data.query_text));

    // non text limitt
    buffers.push(parser.writeInt(data.non_text_limit || -1));

    // fetchplan
    buffers.push(parser.writeString(data.fetchplan || ""));

    // TODO serialized params
    //buffers.push(parser.writeInt(data.serialized_params));
    buffers.push(parser.writeInt(0));

    // append all buffers into one
    var length = buffers.length;
    for (var i = 0, size = 0; i < length; i++) {
        size += buffers[i].length;
    }
    var buffer = new Buffer(size);
    for (var i = 0, size = 0; i < length; i++) {
        size += buffers[i].copy(buffer, size);
    }

    // serialized command
    socket.write(parser.writeBytes(buffer));
};


function readPayloadCollection(buf, offset) {
    var result = this.result,
        objOffset = { offset: offset, limit: result.count },
        records = parser.readCollection(buf, objOffset);

    // null check is NOT missing
    if (typeof records === "undefined") {
        return 0;
    }

    result.count = result.count || objOffset.limit;
    result.content = result.content || [];
    result.content = result.content.concat(records);

    if (result.content.length === result.count) {
        this.step = this.steps.length;
    }

    return objOffset.offset - offset;
}


function readPayloadRecord(buf, offset) {
    var result = this.result,
        objOffset = { offset: offset };

    var content = parser.readRecord(buf, objOffset);
    // null check is NOT missing
    if (typeof content === "undefined") {
        return 0;
    }
    result.content = content;

    this.step = this.steps.length;

    return objOffset.offset - offset;
}


function readPayloadStatus(buf, offset) {
    if (!parser.canReadByte(buf, offset)) {
        return 0;
    }
    var result = this.result;

    result.status = parser.readByte(buf, offset);

    switch (result.status) {
        case 0:
        case 110: // 'n'
            // nothing to do
            this.step = this.steps.length;
            break;
        case 1:
            // TODO
            this.error = new Error("Not implemented!");
            break;
        case 2:
            // TODO
            this.error = new Error("Not implemented!");
            break;
        case 97: // 'a'
            this.step = markers.readPayloadString;
            break;
        case 108: // 'l'
            this.step = markers.readPayloadCollection;
            break;
        case 114: // 'r'
            this.step = markers.readPayloadRecord;
            break;
        default:
            this.step = this.steps.length;
    }

    return parser.BYTES_BYTE; // byte read
}


function readPayloadString(buf, offset) {
    if (!parser.canReadString(buf, offset)) {
        return 0;
    }
    var result = this.result;

    var readString = parser.readString(buf, offset);
    result.content = readString.value;
    result.type = "f";

    this.step = this.steps.length;

    return parser.BYTES_INT + readString.lengthInBytes;
}
