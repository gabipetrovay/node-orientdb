var assert = require("assert");

var orient = require("../lib/orientdb"),
    Db = orient.Db,
    Server = orient.Server;

var serverConfig = require("../config/test/serverConfig");
var dbConfig = require("../config/test/dbConfig");

var server = new Server(serverConfig);
var db = new Db("temp", server, dbConfig);


db.open(function(err, result) {

    assert(!err, "Error while opening the database: " + err);

    var clusterIds = [];

    for (var index in db.clusters) {
        clusterIds.push(db.clusters[index].id);
    }

    db.countDataClusters(clusterIds, function(err, result) {

        assert(!err, "Error while counting data clusters: " + err);

        assert(typeof result.clusterCount === "number", "Was expecting numeric value. Received " + (typeof result.clusterCount));

        console.log("Database \"" + db.databaseName + "\" has " + result.clusterCount + " clusters");

        db.close();
    });
});

