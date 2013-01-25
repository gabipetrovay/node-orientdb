"use strict";

var Server = require("./connection/server").Server,
    OperationTypes = require("./commands/operation_types"),
    parser = require("./connection/parser"),
    _ = require("underscore");

function EMPTY_FUNCTION() {
}

var Db = exports.Db = function(databaseName, server, options) {

    if (!_.isString(databaseName)) {
        throw new Error("Database name was not provided.");
    }

    this.databaseName = databaseName;

    if (server instanceof Server) {
        this.server = server;
    } else {
        // TODO
        throw new Error("Not implemented: server configuration as object literal not yet supported");
    }

    this.options = options || {};
    this.userName = options.user_name;
    this.userPassword = options.user_password;
    this.databaseType = options.database_type || "document";
    this.storageType = options.storage_type || "local";
    this.clusters = [];

    this.transactionId = 0;
};


Db.prototype.getClusterByName = function(clusterName) {

    var lowerCaseClusterName = clusterName.toLowerCase();

    for (var i = 0, l = this.clusters.length; i < l; ++i) {
        if (this.clusters[i].name === lowerCaseClusterName) {
            return this.clusters[i];
        }
    }

    return null;
};

Db.prototype.getClusterById = function(clusterId) {

    for (var i = 0, l = this.clusters.length; i < l; ++i) {
        if (this.clusters[i].id === clusterId) {
            return this.clusters[i];
        }
    }

    return null;
};

Db.prototype.getDataSegmentById = function(dataSegmentId) {

    var dataSegments = this.configuration.dataSegments;

    for (var i = 0, l = dataSegments.length; i < l; ++i) {
        if (dataSegments[i].dataId === dataSegmentId) {
            return dataSegments[i];
        }
    }

    return null;
};


Db.prototype.getClusterByClass = function(className) {

    var clazz = this.getClassByName(className);
    if (clazz) {
        for (var i = 0, l = this.clusters.length; i < l; ++i) {
            if (this.clusters[i].id === clazz.defaultClusterId) {
                return this.clusters[i];
            }
        }
        throw new Error("Default clusterId " + clazz.defaultClusterId + " for class " + className + " is not present");
    }

    return null;
};


Db.prototype.getClassByName = function(className) {

    for (var i = 0, l = this.classes.length; i < l; ++i) {
        if (this.classes[i].name === className || this.classes[i].shortName === className) {
            return this.classes[i];
        }
    }

    return null;
};


function loadDbSchema(self, callback) {
    callback = callback || EMPTY_FUNCTION;

    self.loadRecord("#0:0", function(err, result) {

        if (err) { return callback(err); }

        self.configuration = parser.parseConfiguration(result.data.toString());

        self.loadRecord(self.configuration.schemaRecordId, function(err, result) {

            if (err) { return callback(err); }

            self.schema = result.schema;
            self.classes = result.classes;

            // only now call the open callback
            callback();
        });
    });
}


Db.prototype.open = function(callback) {
    var self = this;
    callback = callback || EMPTY_FUNCTION;

    // for the open operation we want to do some DB and server initialization
    var openCallback = function(err, result) {

        if (err) { return callback(err); }

        // save the session ID in the server and the clusters in the DB
        // TODO the clusters also have to be updated with the cluster commands
        self.server.sessionId = result.sessionId;
        self.clusters = result.clusters;

        // retrieve the DB schema and save it
        // TODO the schema also needs to be update if this changes
        //      (I am not sure if this is currently supported in the protocol)
        loadDbSchema(self, callback);
    };

    var data = {
        database_name: self.databaseName,
        user_name: self.userName,
        user_password: self.userPassword,
        database_type: self.databaseType
    };

    self.server.init(function(err) {

        if (err) { return callback(err); }

        self.server.send(OperationTypes.DB_OPEN, data, openCallback);
    });
};


Db.prototype.create = function(callback) {
    callback = callback || EMPTY_FUNCTION;

    var data = {
        database_name: this.databaseName,
        database_type: this.databaseType,
        storage_type: this.storageType
    };

    this.server.send(OperationTypes.DB_CREATE, data, callback);
};


Db.prototype.close = function(callback) {
    callback = callback || EMPTY_FUNCTION;

    this.server.send(OperationTypes.DB_CLOSE, callback);
};


Db.prototype.exist = function(callback) {
    callback = callback || EMPTY_FUNCTION;

    var data = {
        database_name: this.databaseName
    };

    this.server.send(OperationTypes.DB_EXIST, data, function(err, result) {

        if (err) { return callback(err); }

        result = result || {};
        callback(null, result.result);
    });

};


Db.prototype.reload = function(callback) {
    var self = this;
    callback = callback || EMPTY_FUNCTION;

    var reloadCallback = function(err, result) {

        if (err) { return callback(err); }

        self.clusters = result.clusters;

        loadDbSchema(self, callback);
    };

    this.server.send(OperationTypes.DB_RELOAD, reloadCallback);
};


Db.prototype.drop = function(callback) {
    callback = callback || EMPTY_FUNCTION;

    var data = {
        database_name: this.databaseName
    };

    this.server.send(OperationTypes.DB_DROP, data, callback);
};


Db.prototype.size = function(callback) {
    callback = callback || EMPTY_FUNCTION;

    this.server.send(OperationTypes.DB_SIZE, function(err, result) {

        if (err) { return callback(err); }

        result = result || {};
        callback(null, result.size);
    });
};


Db.prototype.countRecords = function(callback) {
    callback = callback || EMPTY_FUNCTION;

    this.server.send(OperationTypes.DB_COUNTRECORDS, function(err, result) {

        if (err) { return callback(err); }

        result = result || {};
        callback(null, result.count);
    });
};


Db.prototype.command = function(command, options, callback) {
    
    if (_.isFunction(options)) {
        callback = options;
        options = {};
    }
    
    callback = callback || EMPTY_FUNCTION;
    options = options || {};

    var data = {
        queryText: command,
        mode: "s",
        fetchPlan: "",
        nonTextLimit: -1,
        className: "com.orientechnologies.orient.core.sql.OCommandSQL"
    };
    if (_.isString(options.fetchPlan)) {
        data.fetchPlan = options.fetchPlan;
        data.mode = "a";
    }
    if (_.isNumber(options.nonTextLimit)) {
        data.nonTextLimit = options.nonTextLimit;
    }
    if (data.mode === "a") {
        data.className = "com.orientechnologies.orient.core.sql.query.OSQLAsynchQuery";
    }

    var self = this;

    self.server.send(OperationTypes.COMMAND, data, function(err, results) {

        if (err) { return callback(err); }

        results = processResults(results);

        toRidsFromORIDsOfDocuments(self, results, callback);
    });
};


Db.prototype.addDataCluster = function(clusterOptions, callback) {
    callback = callback || EMPTY_FUNCTION;

    var data = {};

    switch (clusterOptions.type) {
        case "PHYSICAL":
        case "MEMORY":
            data.type = clusterOptions.type;
            break;
        default:
            data.type = "MEMORY";
    }

    data.name = clusterOptions.name;

    if (data.type === "PHYSICAL") {
        data.file_name = clusterOptions.file_name || "";
    }

    data.dataSegmentName = clusterOptions.dataSegmentName || null;

    this.server.send(OperationTypes.DATACLUSTER_ADD, data, function(err, result) {

        if (err) { return callback(err); }

        callback(null, result.number);
    });
};


Db.prototype.removeDataCluster = function(clusterNumber, callback) {
    callback = callback || EMPTY_FUNCTION;

    var data = {
        cluster_number: clusterNumber
    };

    this.server.send(OperationTypes.DATACLUSTER_DROP, data, callback);
};


Db.prototype.countDataClusters = function(clustersId, options, callback) {

    callback = callback || EMPTY_FUNCTION;

    var data = {
        clustersId: clustersId
    };

    this.server.send(OperationTypes.DATACLUSTER_COUNT, data, callback);
};


Db.prototype.rangeDataClusters = function(clusterId, callback) {
    callback = callback || EMPTY_FUNCTION;

    var data = {
        clusterId: clusterId
    };

    this.server.send(OperationTypes.DATACLUSTER_DATARANGE, data, callback);
};


Db.prototype.createRecord = function(recordData, callback) {
    callback = callback || EMPTY_FUNCTION;

    var recordType = parser.decodeRecordType(recordData.type);

    if (!recordType) {
        return callback(new Error("Invalid record type. Valid values are: b, d, and f"));
    }

    var data = {
        dataSegmentId: recordData.dataSegmentId,
        clusterId: recordData.clusterId,
        content: recordData.content,
        type: recordType,
        mode: 0
    };

    this.server.send(OperationTypes.RECORD_CREATE, data, callback);
};


function recursivelyAccumulateORIDsTuples(document, tuples) {
    for (var field in document) {
        if (!document.hasOwnProperty(field)) {
            continue;
        }
        var fieldValue = document[field];
        if (_.isObject(fieldValue) && !parser.isNullOrUndefined(fieldValue["@class"])) {
            if (fieldValue["@class"] === "ORIDs") {
                var tuple = { doc: document, field: field, value: fieldValue };
                tuples.push(tuple);
            } else {
                recursivelyAccumulateORIDsTuples(fieldValue, tuples);
            }
        } else if (_.isArray(fieldValue)) {
            for (var i = 0, l = fieldValue.length; i < l; ++i) {
                recursivelyAccumulateORIDsTuples(fieldValue[i], tuples);
            }
        }
    }
}


function fromORIDTupleToRids(self, tuple, ridOfORID, size, rids, callback) {
    self.loadRecord(ridOfORID, function(err, ORID) {
        if (err) { return callback(err); }

        var treeNode = parser.deserializeORID(ORID);
        size = size || treeNode.treeSize;
        parser.pushArray(rids, treeNode.rids);

        if (rids.length === size) {
            callback(null, tuple, rids);
        }

        if (treeNode.leftRid) {
            fromORIDTupleToRids(self, tuple, treeNode.leftRid, size, rids, callback);
        }
        if (treeNode.rightRid) {
            fromORIDTupleToRids(self, tuple, treeNode.rightRid, size, rids, callback);
        }
    });
}


function toRidsFromORIDsOfDocument(self, document, callback) {
    if (parser.isNullOrUndefined(document) || document["@type"] !== "d") {
        return callback(null, document);
    }

    var oRIDsTuples = [];
    recursivelyAccumulateORIDsTuples(document, oRIDsTuples);

    if (oRIDsTuples.length === 0) {
        return callback(null, document);
    }

    var loadedTuples = 0;
    for (var i = 0, l = oRIDsTuples.length; i < l; ++i) {
        var tuple = oRIDsTuples[i];
        fromORIDTupleToRids(self, tuple, tuple.value.root, 0, [], function(err, tuple, rids) {
            if (err) { return callback(err); }

            tuple.doc[tuple.field] = rids;
            loadedTuples++;

            if (loadedTuples === oRIDsTuples.length) {
                return callback(null, document);
            }
        });
    }
}


function toRidsFromORIDsOfDocuments(self, documents, callback) {
    if (documents.length === 0) {
        return callback(null, documents);
    }

    var done = 0;
    for (var i = 0, l = documents.length; i < l; ++i) {
        toRidsFromORIDsOfDocument(self, documents[i], function(err, document) {
            if (err) { return callback(err); }
            
            done++;
            if (done === documents.length) {
                return callback(null, documents);
            }
        });
    }
}


Db.prototype.loadRecord = function(rid, options, callback) {

    if (_.isFunction(options)) {
        callback = options;
        options = {};
    }

    callback = callback || EMPTY_FUNCTION;

    // TODO allow also rid as object literal (probably merged with options)
    if (!parser.canParseRid(rid)) {
        return callback(new Error("The RID must be a string having the format: #<cluster id>:<cluster position>"));
    }
    var data = parser.parseRid(rid);
    data.fetchPlan = _.isString(options.fetchPlan) ? options.fetchPlan : "";
    data.ignoreCache = _.isNumber(options.ignoreCache) ? options.ignoreCache : 0;

    var self = this;

    self.server.send(OperationTypes.RECORD_LOAD, data, function(err, result) {

        if (err) { return callback(err); }

        if (!result) { return callback(new Error("A null or undefined record was returned.")); }

        if (result.status === 0) { return callback(new Error("Record " + rid + " does not exist")); }

        // TODO review response of RecordLoad
        var record = processResult(result, result.subcontents);
        record["@rid"] = rid;

        toRidsFromORIDsOfDocument(self, record, callback);
    });
};

Db.prototype.loadRecords = function(rids, options, callback) {
    if (_.isFunction(options)) {
        callback = options;
        options = {};
    }

    var records = [];
    var ridsLength = rids.length;
    if (ridsLength === 0) {
        return callback(null, records);
    }
    for (var i = 0; i < ridsLength; i++) {
        this.loadRecord(rids[i], options, function(err, record) {
            if (err) { return callback(err); }
            records.push(record);

            if (records.length === ridsLength) {
                return callback(null, records);
            }
        });
    }
};

function processResults(results) {

    var records = [];
    var content = results.content;
console.log(results);
    switch (results.status) {

        case 97: // 'a'
            content["@type"] = "f";
            records.push(content);
            break;

        case 1:
        case 108: // 'l'
        case 114: // 'r'
            for (var i = 0, l = content.length; i < l; ++i) {
                records.push(processResult(content[i], results.subcontents));
            }
            break;

        case 0:
        case 110: // 'n' 
            //nothing to do
            break;

        default:
            throw new Error("Unexpected or unimplemented result status: " + results.status);
    }

    return records;
}

function processResult(content, subcontents) {

    var record;

    switch (content.type) {

        case "b":
        case "f":
            record = {
                "@type": content.type,
                "@version": content.version,
                data: content.content
            };

            break;

        case "d":
            if (parser.isNullOrUndefined(content.content)) {
                record = {};
            } else {
                record = parser.deserializeDocument(content.content.toString());
            }
            record["@type"] = "d";
            if (!parser.isNullOrUndefined(content.version)) {
                record["@version"] = content.version;
            }
            record["@rid"] = content.rid;

            if (subcontents) {
                var subRecords = [];
                for (var i = 0, l = subcontents.length; i < l; ++i) {
                    var subRecord = processResult(subcontents[i]);
                    subRecords.push(subRecord);
                }
                recursivelyReplaceSubRecordsInRecord(record, subRecords, [record["@rid"]]);
            }

            break;

        default:
            throw new Error("Invalid or not supported record type: " + content.type);
    }

    return record;
}

function recursivelyReplaceSubRecordsInRecord(currentRecord, subRecords, accumulatedRIDs) {
    var ridsInRecord = [];
    collectRIDsIn(currentRecord, ridsInRecord);
    for (var i = 0, l = ridsInRecord.length; i < l; ++i) {
        var ridInRecord = ridsInRecord[i];
        if (accumulatedRIDs.indexOf(ridInRecord.rid) === -1) {
            var subRecord = _.find(subRecords, function(record) {
                return record["@rid"] === ridInRecord.rid;
            });
            if (!parser.isNullOrUndefined(subRecord)) {
                ridInRecord.container[ridInRecord.key] = parser.deepClone(subRecord);
                subRecord = ridInRecord.container[ridInRecord.key];
                var newAccumulatedRIDs = accumulatedRIDs.slice(0);
                newAccumulatedRIDs.push(subRecord["@rid"]);
                recursivelyReplaceSubRecordsInRecord(subRecord, subRecords, newAccumulatedRIDs);
            }
        }
    }
}

function collectRIDsIn(obj, accumulator, container, key) {
    if (_.isArray(obj)) {
        for (var i = 0, l = obj.length; i < l; ++i) {
            collectRIDsIn(obj[i], accumulator, obj, i);
        }
    } else if (_.isObject(obj)) {
        for (var field in obj) {
            if (obj.hasOwnProperty(field) && field[0] !== "@") {
                collectRIDsIn(obj[field], accumulator, obj, field);
            }
        }
    } else if (parser.canParseRid(obj)) {
        accumulator.push({ rid: obj, container: container, key: key });
    }
}

Db.prototype.updateRecord = function(recordData, callback) {
    callback = callback || EMPTY_FUNCTION;

    var recordType = parser.decodeRecordType(recordData.type);

    if (!recordType) {
        return callback(new Error("Invalid record type. Valid values are: b, d, and f"));
    }

    var data = {
        clusterId: recordData.clusterId,
        clusterPosition: recordData.clusterPosition,
        content: recordData.content,
        type: recordType,
        mode: 0
    };

    if (_.isNumber(recordData.version)) {
        data.version = recordData.version;
    } else {
        data.version = -1;
    }

    this.server.send(OperationTypes.RECORD_UPDATE, data, callback);
};

Db.prototype.delete = function(document, transaction, callback) {
    if (_.isFunction(transaction)) {
        callback = transaction;
        transaction = null;
    }

    callback = callback || EMPTY_FUNCTION;

    if (transaction) {
        var docsIndex = transaction.docs.indexOf(document);
        if (docsIndex === -1) {
            transaction.docs.push(document);
            transaction.actions.push("delete");
        } else if (transaction.actions[docsIndex] !== "delete") {
            callback(new Error("Doc has already been marked for action " + transaction.actions[docsIndex]));
            return;
        }
        callback();
    } else {
        var recordData = parser.parseRid(document["@rid"]);
        recordData.version = document["@version"];

        this.deleteRecord(recordData, callback);
    }
};

Db.prototype.deleteRecord = function(recordData, callback) {
    callback = callback || EMPTY_FUNCTION;

    var data = {
        clusterId: recordData.clusterId,
        clusterPosition: recordData.clusterPosition,
        version: recordData.version,
        mode: 0
    };

    this.server.send(OperationTypes.RECORD_DELETE, data, callback);
};


Db.prototype.save = function(document, transaction, callback) {
    if (_.isFunction(transaction)) {
        callback = transaction;
        transaction = null;
    }

    callback = callback || EMPTY_FUNCTION;

    if (parser.isNullOrUndefined(document["@class"])) {
        return callback(new Error("Document must have a class"));
    }

    var self = this;

    var recordData = {
        type: "d"
    };

    if (!parser.isNullOrUndefined(document["@rid"])) {
        var rid = document["@rid"];
        if (!parser.canParseRid(rid)) {
            return callback(new Error("The RID must be a string having the format: #<cluster id>:<cluster position>"));
        }
        _.extend(recordData, parser.parseRid(rid));
    }

    if (!parser.isNullOrUndefined(document["@version"])) {
        recordData.version = document["@version"];
    }

    recordData.content = parser.stringToBuffer(parser.serializeDocument(document));

    var saveDocument = function(recordData) {

        if (transaction) {
            var docsIndex = transaction.docs.indexOf(document);
            if (docsIndex === -1) {
                var action;
                if (parser.isNullOrUndefined(document["@rid"])) {
                    document["@rid"] = parser.toRid(self.getClusterByClass(document["@class"]).id, --transaction.clusterPosition);
                    action = "create";
                } else {
                    action = "update";
                }
                transaction.docs.push(document);
                transaction.actions.push(action);
            } else if (transaction.actions[docsIndex] === "delete") {
                callback(new Error("Doc has already been marked for deletion"));
                return;
            }
            callback(null, document);
        } else if (!parser.isNullOrUndefined(recordData.clusterPosition)) {
            self.updateRecord(recordData, function(err) {

                if (err) { return callback(err); }

                self.loadRecord("#" + recordData.clusterId + ":" + recordData.clusterPosition, callback);
            });
        } else {
            self.createRecord(recordData, function(err, result) {

                if (err) { return callback(err); }

                self.loadRecord("#" + recordData.clusterId + ":" + result.position, callback);
            });
        }
    };

    var cluster = this.getClusterByClass(document["@class"]);

    if (cluster !== null) {
        recordData.clusterId = cluster.id;
        recordData.dataSegmentId = cluster.dataSegmentId;
        saveDocument(recordData);
    } else {
        self.createClass(document["@class"], function(err, clusterId) {

            if (err) { return callback(err); }

            recordData.clusterId = clusterId;
            recordData.dataSegmentId = self.getClusterById(clusterId).dataSegmentId;
            saveDocument(recordData);
        });
    }
};

Db.prototype.cascadingSave = function(masterDocument, transaction, callback) {
    if (_.isFunction(transaction)) {
        callback = transaction;
        transaction = null;
    }

    callback = callback || EMPTY_FUNCTION;

    var self = this;

    function recursivelyFindObjectsWithClass(document, path, pathsAccumulator) {
        for (var field in document) {
            if (!document.hasOwnProperty(field)) {
                continue;
            }
            var fieldValue = document[field];
            if (_.isObject(fieldValue) || _.isArray(fieldValue)) {
                var currentPath;
                if (path !== "") {
                    currentPath = path.concat(".", field);
                } else {
                    currentPath = field;
                }
                if (_.isObject(fieldValue)) {
                    recursivelyFindObjectsWithClass(fieldValue, currentPath, pathsAccumulator);
                } else if (_.isArray(fieldValue)) {
                    for (var i = 0, l = fieldValue.length; i < l; ++i) {
                        var currentPathWithIdx = currentPath.concat(".", i);
                        recursivelyFindObjectsWithClass(fieldValue[i], currentPathWithIdx, pathsAccumulator);
                    }
                }
            }
        }
        if (document["@class"]) {
            pathsAccumulator.push(path);
        }
    }

    var orderedPaths = [];
    recursivelyFindObjectsWithClass(masterDocument, "", orderedPaths);

    if (orderedPaths.length === 0) {
        callback(new Error("Document cannot be saved"));
        return;
    }

    function subElementByPath(document, pathParts, maxDepth) {
        if (parser.isNullOrUndefined(maxDepth)) {
            maxDepth = pathParts.length;
        }
        for (var i = 0; i < maxDepth && pathParts[i] !== ""; i++) {
            document = document[pathParts[i]];
        }
        return document;
    }

    function replaceElementAtPath(document, pathParts, replacement) {
        document = subElementByPath(document, pathParts, pathParts.length - 1);
        document[pathParts[pathParts.length - 1]] = replacement;
    }

    function saveDocumentsByPaths(masterDocument, orderedPaths, savedDocumentsAccumulator, callback) {
        var pathParts = orderedPaths.shift().split(".");
        var document = subElementByPath(masterDocument, pathParts);
        self.save(document, function(err, savedDocument) {
            if (err) { return callback(err); }

            savedDocumentsAccumulator.push(savedDocument);

            if (orderedPaths.length > 0) {
                replaceElementAtPath(masterDocument, pathParts, savedDocument["@rid"]);
                saveDocumentsByPaths(masterDocument, orderedPaths, savedDocumentsAccumulator, callback);
            } else {
                callback();
            }
        });
    }

    function rebuildMasterDocumentFromSavedDocuments(orderedPaths, orderedSavedDocs) {
        //the last saved doc is the saved version of the input master document
        var savedMasterDocument = orderedSavedDocs[orderedSavedDocs.length - 1];

        for (var i = orderedPaths.length - 2; i >= 0; i--) {
            replaceElementAtPath(savedMasterDocument, orderedPaths[i].split("."), orderedSavedDocs[i]);
        }
        return savedMasterDocument;
    }

    var orderedSavedDocs = [];
    saveDocumentsByPaths(masterDocument, orderedPaths.slice(0), orderedSavedDocs, function(err) {
        if (err) { return callback(err); }

        var newMasterDocument = rebuildMasterDocumentFromSavedDocuments(orderedPaths, orderedSavedDocs);

        callback(null, newMasterDocument);
    });
};

Db.prototype.createClass = function(className, superClassName, callback) {
    if (_.isFunction(superClassName)) {
        callback = superClassName;
        superClassName = null;
    }

    var cluster = this.getClusterByName("default");
    var dataSegment = this.getDataSegmentById(cluster.dataSegmentId);

    var clusterOptions = {
        type: cluster.type,
        name: className,
        dataSegmentName: (dataSegment !== null ? dataSegment.dataName : "internal")
    };

    var self = this;

    self.addDataCluster(clusterOptions, function(err, clusterId) {

        if (err) { return callback(err); }

        var createClassCommand = "CREATE CLASS ".concat(className);
        if (superClassName) {
            createClassCommand = createClassCommand.concat(" extends ").concat(superClassName);
        }
        createClassCommand = createClassCommand.concat(" cluster ").concat(clusterId);
        self.command(createClassCommand, function(err) {

            if (err) { return callback(err); }

            self.reload(function(err) {

                if (err) { return callback(err); }

                callback(null, clusterId);
            });
        });
    });
};

Db.prototype.dropClass = function(className, callback) {
    callback = callback || EMPTY_FUNCTION;

    var self = this;

    self.command("DROP CLASS " + className, function(err) {

        if (err) { return callback(err); }

        self.reload(function(err) {

            if (err) { return callback(err); }

            callback();
        });
    });
};

Db.prototype.countRecordsInCluster = function(clusterName, callback) {
    callback = callback || EMPTY_FUNCTION;

    this.server.send(OperationTypes.COUNT, clusterName, function(err, result) {

        if (err) { return callback(err); }

        result = result || {};
        callback(null, result.count);
    });
};

Db.prototype.begin = function() {
    var transaction = {
        id: this.transactionId,
        clusterPosition: -1,
        actions: [],
        docs: []
    };

    this.transactionId++;

    return transaction;
};

Db.prototype.rollback = function(transaction) {
    for (var key in transaction) {
        if (transaction.hasOwnProperty(key)) {
            delete transaction[key];
        }
    }
};

Db.prototype.commit = function(transaction, callback) {
    if (parser.isNullOrUndefined(transaction.id) || parser.isNullOrUndefined(transaction.clusterPosition) || parser.isNullOrUndefined(transaction.actions) || transaction.actions.length === 0) {
        callback(new Error("Invalid transaction: " + JSON.stringify(transaction)));
    }

    callback = callback || EMPTY_FUNCTION;

    this.server.send(OperationTypes.TX_COMMIT, transaction, function(err, result) {

        if (err) { return callback(err); }

        result = result || {};
        callback(null, result);
    });
};
