"use strict";

var parser = require("../connection/parser");

var command = function() {
    this.step = 0;

    this.steps = [];
    this.steps.push(command.skipByte);
    this.steps.push(command.skipInt);

    this.result = {};
    this.error = null;
};

command.prototype.done = function() {
    return this.error !== null || this.step >= this.steps.length;
};

command.prototype.read = function(buf) {
    var bytesRead		= 0,
        bytesLingering	= (this.lingeringBuffer && this.lingeringBuffer.length) || 0,
        totalBytesRead	= bytesRead,
        localBuffer		= new Buffer(buf.length + bytesLingering);

    if(bytesLingering) {
        this.lingeringBuffer.copy(localBuffer);
    }

    buf.copy(localBuffer, bytesLingering);

    while(!this.done() && (bytesRead = command.callStep(this, localBuffer, totalBytesRead))) {
        totalBytesRead += bytesRead;
    }

    this.lingeringBuffer = new Buffer(localBuffer.length - totalBytesRead);

    localBuffer.copy(this.lingeringBuffer, 0, totalBytesRead);

    // Give back the remaining buffer to the caller
    return this.lingeringBuffer;
};

command.callStep = function(self, localBuffer, totalBytesRead) {
    return self.steps[self.step].call(self, localBuffer, totalBytesRead);
};

command.skipByte = function(buf, offset) {
    if (!parser.canReadByte(buf, offset)) {
        return 0;
    }

    this.step++;
    return parser.BYTES_BYTE;
};

command.skipInt = function(buf, offset) {
    if (!parser.canReadInt(buf, offset)) {
        return 0;
    }
    this.step++;
    return parser.BYTES_INT;
};

module.exports = command;