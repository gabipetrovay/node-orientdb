var assert = require("assert");
var parser = require("../lib/orientdb/connection/parser");
var _ = require("lodash");

var orient = require("../lib/orientdb"),
    GraphDb = orient.GraphDb,
    Server = orient.Server;

var serverConfig = require("../config/test/serverConfig");
var dbConfig = require("../config/test/dbConfig");

var server = new Server(serverConfig);
var graphdb = new GraphDb("temp", server, dbConfig);

graphdb.open(function(err) {
    assert(!err, err);

    graphdb.createVertex({}, function(err, vertex1) {
        assert(!err, err);

        assert(!_.isUndefined(vertex1["@rid"]));

        graphdb.createVertex({}, function(err, vertex2) {
            assert(!err, err);

            assert(!_.isUndefined(vertex2["@rid"]));

            graphdb.createEdge(vertex1, vertex2, { name: "select_flatten" }, function(err, edge) {
                assert(!err, err);

                assert(!_.isUndefined(edge["@rid"]));
                
                var outProperty = "out";
                var sqlCommand = "flatten";
                if (server.manager.serverProtocolVersion >= 14) {
                    outProperty = "out_";
                    sqlCommand = "expand";
                }

                graphdb.command("select " + sqlCommand + "(" + outProperty + "[name = \"select_flatten\"].in) from V", function(err, results) {
                    assert(!err, err);

                    assert.equal(1, results.length);
                    assert(!_.isUndefined(results[0]["@rid"]));
                    assert(!_.isUndefined(results[0]["@type"]));

                    graphdb.close();
                });
            });
        });
    });
});

