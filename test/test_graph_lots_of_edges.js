var assert = require("assert");

var orient = require("../lib/orientdb"),
    GraphDb = orient.GraphDb,
    Server = orient.Server;

var serverConfig = require("../config/test/serverConfig");
var dbConfig = require("../config/test/dbConfig");

var server = new Server(serverConfig);
var graphdb = new GraphDb("temp", server, dbConfig);


graphdb.open(function(err) {
    assert(!err, err);

    graphdb.createVertex({ name: "from vertex" }, function(err, fromVertex) {
        assert(!err, err);

        graphdb.createVertex({ name: "to vertex" }, function(err, toVertex) {
            assert(!err, err);

            var edges = [];
            for (var idx = 0; idx < 50; idx++) {
                graphdb.createEdge(fromVertex["@rid"], toVertex["@rid"], function(err, edge) {
                    assert(!err, err);

                    edges.push(edge["@rid"]);

                    if (edges.length === 50) {
                        graphdb.loadRecord(fromVertex["@rid"], function(err, fromVertex) {
                            assert(!err, err);

                            if (server.manager.serverProtocolVersion < 14) {
                                assert.equal(50, fromVertex.out.length);
                            } else {
                                assert.equal(50, fromVertex.out_.length);
                            }

                            graphdb.command("select from " + fromVertex["@rid"], function(err, results) {
                                assert(!err, err);

                                if (server.manager.serverProtocolVersion < 14) {
                                    assert.equal(50, results[0].out.length);
                                } else {
                                    assert.equal(50, results[0].out_.length);
                                }
                                
                                graphdb.close();
                            });
                        });
                    }
                });
            }
        });
    });
});

