"use strict";

var util = require("util"),
    Db = require("./db").Db,
    parser = require("./connection/parser"),
    _ = require("lodash");

var VERTEX_CLASS_NAME = "V";
var EDGE_CLASS_NAME = "E";
var PERSISTENT_CLASS_NAME = "ORIDs";
var FIELD_IN = "in";
var FIELD_OUT = "out";

var GraphDb = exports.GraphDb = function(databaseName, server, options) {
    Db.call(this, databaseName, server, options);
};

util.inherits(GraphDb, Db);

function checkForGraphSchema(self, callback) {

    var checkORIDs = function(callback) {
        var cluster = self.getClusterByClass(PERSISTENT_CLASS_NAME);
        if (cluster === null) {
            self.createClass("ORIDs", callback);
        } else {
            return callback();
        }
    };

    var checkClassWithShortName = function(fieldName, className, classShortName, inOutFieldType, callback) {
        self[fieldName] = self.getClassByName(className);
        if (parser.isNullOrUndefined(self[fieldName])) {
            self.createClass(className, function(err) {
                if (err) { return callback(err); }

                self.command("ALTER CLASS " + className + " SHORTNAME " + classShortName, function(err) {
                    if (err) { return callback(err); }

                    self.command("CREATE PROPERTY " + className + ".in " + inOutFieldType, function(err) {
                        if (err) { return callback(err); }

                        self.command("CREATE PROPERTY " + className + ".out " + inOutFieldType, callback);
                    });
                });
            });
        } else {
            return callback();
        }
    };

    var checkVertexClass = function(callback) {
        checkClassWithShortName("vertexClass", VERTEX_CLASS_NAME, "V", "LINKSET", function(err) {
            if (err) { return callback(err); }

            self.command("ALTER CLASS " + VERTEX_CLASS_NAME + " OVERSIZE 2", callback);
        });
    };

    checkORIDs(function(err) {
        if (err) { return callback(err); }

        checkVertexClass(function(err) {
            if (err) { return callback(err); }

            checkClassWithShortName("edgeClass", EDGE_CLASS_NAME, "E", "LINK", function(err) {
                if (err) { return callback(err); }

                self.reload(callback);
            });
        });
    });
}

GraphDb.prototype.open = function(callback) {
    var self = this;
    Db.prototype.open.call(self, function(err) {

        if (err) { return callback(err); }
        
        if (self.server.manager.serverProtocolVersion < 14){
            VERTEX_CLASS_NAME = "OGraphVertex";
            EDGE_CLASS_NAME = "OGraphEdge";
        }

        checkForGraphSchema(self, callback);
    });
};

GraphDb.prototype.createVertex = function(hash, options, callback) {
    if (_.isFunction(hash)) {
        callback = hash;
        hash = undefined;
        options = {};
    } else if (_.isFunction(options)) {
        callback = options;
        options = {};
    }

    var clazz = options["class"] || VERTEX_CLASS_NAME;

    var self = this;

    var cluster = self.getClusterByClass(clazz);

    var sqlsets = parser.hashToSQLSets(hash);

    var command = "CREATE VERTEX " + clazz + " CLUSTER " + cluster.name;
    if (sqlsets.sqlsets !== "") {
        command = command.concat(" ", sqlsets.sqlsets);
    }

    self.command(command, function(err, results) {
        if (err) { return callback(err); }

        var vertex = results[0];

        if (_.isEmpty(sqlsets.remainingHash)) {
            callback(null, vertex);
        } else {
            _.extend(vertex, sqlsets.remainingHash);
            self.save(vertex, callback);
        }
    });
};

GraphDb.prototype.createEdge = function(sourceVertexOrRID, destVertexOrRID, hash, options, callback) {
    if (_.isFunction(hash)) {
        callback = hash;
        hash = undefined;
        options = {};
    } else if (_.isFunction(options)) {
        callback = options;
        options = {};
    }

    function ridFrom(obj) {
        if (!_.isString(obj)) {
            return obj["@rid"];
        }
        return obj;
    }

    function pushRIDInto(rid, obj, field) {
        if (_.isString(obj)) {
            return;
        }

        if (obj[field] === null || typeof obj[field] === "undefined") {
            obj[field] = [];
        }
        obj[field].push(rid);
    }

    function pushRIDs(obj, edge, otherObj, field) {
        var rid;
        if (edge["@class"] !== EDGE_CLASS_NAME && self.server.manager.serverProtocolVersion >= 14) {
            field = field + "_" + edge["@class"];
        }
        
        if (!_.isString(obj)) {
            rid = ridFrom(edge);
            if (typeof rid === "undefined") {
                //Assuming that a missing @rid means this is a lightweight edge, in which case
                //the @rid from the vertex at the other end should be used.
                rid = ridFrom(otherObj);
            }
            
            if (self.server.manager.serverProtocolVersion < 14) {
                if (obj[field] === null || typeof obj[field] === "undefined") {
                    obj[field] = [];
                }
                obj[field].push(rid);
            } else {
                if (obj[field] === null || typeof obj[field] === "undefined") {
                    obj[field] = rid;
                } else if (typeof obj[field] === "string") {
                    obj[field] = [obj[field],rid];
                } else if (typeof obj[field] === "array") {
                    obj[field].push(rid);
                }
            }
        }

    }

    var sourceRID = ridFrom(sourceVertexOrRID);
    var destRID = ridFrom(destVertexOrRID);

    var clazz = options["class"] || EDGE_CLASS_NAME;

    var self = this;

    var cluster = self.getClusterByClass(clazz);

    var sqlsets = parser.hashToSQLSets(hash);

    var command = "CREATE EDGE " + clazz + " CLUSTER " + cluster.name + " FROM " + sourceRID + " TO " + destRID;
    if (sqlsets.sqlsets !== "") {
        command = command.concat(" ", sqlsets.sqlsets);
    }

    self.command(command, function(err, results) {
        if (err) { return callback(err); }
        
        //console.dir(results);

        var edge = results[0];

        pushRIDs(sourceVertexOrRID, edge, destVertexOrRID, FIELD_OUT);
        pushRIDs(destVertexOrRID, edge, sourceVertexOrRID, FIELD_IN);
        

        if (_.isEmpty(sqlsets.remainingHash)) {
            callback(null, edge);
        } else {
            _.extend(edge, sqlsets.remainingHash);
            self.save(edge, callback);
        }
    });
};

function getEdgesByDirection(self, sourceVertex, direction, label, callback) {
    if (_.isFunction(label)) {
        callback = label;
        label = undefined;
    }
    

    if (self.server.manager.serverProtocolVersion < 14) {
        var edgesRids = sourceVertex[direction];
        if (!edgesRids || edgesRids.length === 0) {
            return callback(null, []);
        }
    
        var edges = [];
        var loadedEdges = 0;
        var edgesRidsLength = edgesRids.length;
        for (var idx = 0; idx < edgesRidsLength; idx++) {
            self.loadRecord(edgesRids[idx], function(err, edge) {
                if (err) { return callback(err); }
                loadedEdges++;
                if (!label || label === edge.label) {
                    edges.push(edge);
                }
    
                if (loadedEdges === edgesRidsLength) {
                    return callback(null, edges);
                }
            });
        }
    } else {
        var objectRids = [];
        if (!label) {
            var prefix = direction + "_";
            for (var property in sourceVertex) {
                if (property.substring(0,prefix.length) === prefix) {
                    var propertyValue = sourceVertex[property];
                    if (_.isString(propertyValue)) {
                        objectRids.push(propertyValue);
                    } else if (_.isObject(propertyValue) || _.isArray(propertyValue)) {
                        for (var idx in propertyValue) {
                            objectRids.push(propertyValue[idx]);
                        }
                    }
                }
            }
        } else {
            var propertyValue = sourceVertex[direction + "_" + label];
            if (_.isString(propertyValue)) {
                objectRids.push(propertyValue);
            } else if (_.isObject(propertyValue) || _.isArray(propertyValue)) {
                for (var idx in propertyValue) {
                    objectRids.push(propertyValue[idx]);
                }
            }
        }
        var loadedObjects = 0;
        var objectRidsLength = objectRids.length;
        var objects = [];
        for (var idx = 0; idx < objectRidsLength; idx++) {
            self.loadRecord(objectRids[idx], function(err, edgeOrVertex) {
                if (err) { return callback(err); }
                loadedObjects++;
                
                //TODO: Should we offer a way to allow caller to exclude vertexes?
                objects.push(edgeOrVertex);
    
                if (loadedObjects === objectRidsLength) {
                    return callback(null, objects);
                }
            });
        }
    }
}

GraphDb.prototype.getOutEdges = function(sourceVertex, label, callback) {
    getEdgesByDirection(this, sourceVertex, FIELD_OUT, label, callback);
};

GraphDb.prototype.getInEdges = function(sourceVertex, label, callback) {
    getEdgesByDirection(this, sourceVertex, FIELD_IN, label, callback);
};

GraphDb.prototype.getInVertex = function(sourceEdge, callback) {
    this.loadRecord(sourceEdge[FIELD_IN], callback);
};

GraphDb.prototype.getOutVertex = function(sourceEdge, callback) {
    this.loadRecord(sourceEdge[FIELD_OUT], callback);
};

function fieldOfRecords(records, field) {
    var rids = [];
    for (var idx = 0, length = records.length; idx < length; idx++) {
        rids.push(records[idx][field]);
    }
    return rids;
}

GraphDb.prototype.fromVertex = function(sourceVertex) {
    var self = this;

    return {
        inVertexes: function(label, callback) {
            if (_.isFunction(label)) {
                callback = label;
                label = undefined;
            }
            self.getInEdges(sourceVertex, label, function(err, edges) {
                if (err) { return callback(err); }

                self.loadRecords(fieldOfRecords(edges, FIELD_OUT), callback);
            });
        },
        outVertexes: function(label, callback) {
            if (_.isFunction(label)) {
                callback = label;
                label = undefined;
            }
            self.getOutEdges(sourceVertex, label, function(err, edges) {
                if (err) { return callback(err); }

                self.loadRecords(fieldOfRecords(edges, FIELD_IN), callback);
            });
        }
    };
};
