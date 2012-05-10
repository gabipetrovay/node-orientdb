var util = require("util"),
    Db = require("./db").Db,
    parser = require("./connection/parser");

var VERTEX_CLASS_NAME = "OGraphVertex";
var EDGE_CLASS_NAME = "OGraphEdge";
var PERSISTENT_CLASS_NAME = "ORIDs";

var GraphDb = exports.GraphDb = function(databaseName, server, options) {
    Db.call(this, databaseName, server, options);
};

util.inherits(GraphDb, Db);

function checkForGraphSchema(self, callback) {

    var checkORIDs = function(callback) {
        var clusterId = self.getClusterIdByClass(PERSISTENT_CLASS_NAME);
        if (clusterId === -1) {
            self.createClass("ORIDs", callback);
        } else {
            callback();
        }
    };

    var checkClassWithShortName = function(fieldName, className, classShortName, callback) {
        self[fieldName] = self.getClassByName(className);
        if (self[fieldName] === null || typeof self[fieldName] === "undefined") {
            self.createClass(className, function(err) {
                if (err) { return callback(err); }

                self.command("alter class " + className + " shortname " + classShortName, callback);
            });
        } else {
            callback();
        }
    };

    var checkVertexClass = function(callback) {
        checkClassWithShortName("vertexClass", VERTEX_CLASS_NAME, "V", function(err) {
            if (err) { return callback(err); }

            self.command("alter class " + VERTEX_CLASS_NAME + " oversize 2", callback);
        });
    };

    checkORIDs(function(err) {
        if (err) { return callback(err); }

        checkVertexClass(function(err) {
            if (err) { return callback(err); }

            checkClassWithShortName("edgeClass", EDGE_CLASS_NAME, "E", callback);
        });
    });
}

GraphDb.prototype.open = function(callback) {
    var self = this;
    Db.prototype.open.call(self, function(err) {

        if (err) { return callback(err); }

        checkForGraphSchema(self, callback);
    });
};

function createGraphElement(self, element, hash, callback) {
    if (typeof hash === "object") {
        parser.mergeHashes(element, hash);
    }
    if (typeof hash === "function") {
        callback = hash;
    }

    if (callback === null || typeof callback === "undefined") {
        return element;
    }
    self.save(element, callback);
}

GraphDb.prototype.createVertex = function(hash, callback) {
    return createGraphElement(this, {
        "@class": VERTEX_CLASS_NAME
    }, hash, callback);
};

GraphDb.prototype.createEdge = function(sourceVertex, destVertex, hash, callback) {
    var self = this;

    function onEdgeCreated(err, edge) {
        if (err) { return callback(err); }

        if (sourceVertex["out"] === null || typeof sourceVertex["out"] === "undefined") {
            sourceVertex["out"] = [];
        }
        sourceVertex["out"].push(edge["@rid"]);
        self.save(sourceVertex, function(err, savedSourceVertex) {

            if (err) { return callback(err); }

            parser.mergeHashes(sourceVertex, savedSourceVertex);

            if (destVertex["in"] === null || typeof destVertex["in"] === "undefined") {
                destVertex["in"] = [];
            }
            destVertex["in"].push(edge["@rid"]);

            self.save(destVertex, function(err, savedDestVertex) {

                if (err) { return callback(err); }

                parser.mergeHashes(destVertex, savedDestVertex);

                callback(undefined, edge);
            });

        });
    }

    if (typeof hash === "function") {
        callback = hash;
        hash = undefined;
    }

    createGraphElement(self, {
        "@class": EDGE_CLASS_NAME,
        "out": sourceVertex["@rid"],
        "in": destVertex["@rid"]
    }, hash, onEdgeCreated);
};