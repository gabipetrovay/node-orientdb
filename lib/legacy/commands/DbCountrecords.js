'use strict';

var util			= require('util'),
    base			= require('./CommandBase'),
    parser			= require('../connection/parser'),
    OperationTypes	= require('./operation_types'),

    command = function() {
        base.call(this);

        this.steps.push(readCount);
    };

util.inherits(command, base);

module.exports		= command;
command.operation	= OperationTypes.REQUEST_DB_COUNTRECORDS;

function readCount(buf, offset) {
    if (!parser.canReadLong(buf, offset)) {
		this.result	= 0;
        return 0;
    }

    this.result = parser.readLong(buf, offset);
    this.step++;
    return parser.BYTES_LONG;
}

command.write = function(socket, sessionId, data, callback) {
    //Operation type
    socket.write(parser.writeByte(command.operation));

    //Invoke callback immediately when the operation is sent to the server
    callback();

    //Session ID
    socket.write(parser.writeInt(sessionId));
};

