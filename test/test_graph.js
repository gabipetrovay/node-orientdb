var assert = require("assert");

var orient = require("../lib/orientdb"),
    GraphDb = orient.GraphDb,
    Server = orient.Server;

var serverConfig = require("../config/test/serverConfig");
var dbConfig = require("../config/test/dbConfig");

var server = new Server(serverConfig);
var graphdb = new GraphDb("temp", server, dbConfig);

function createVertexes(graphdb, callback) {
    graphdb.createClass("parent_of", "E", function(err, clusterId) {
        assert(!err, err);
        graphdb.createClass("child_of", "E", function(err, clusterId) {
            assert(!err, err);
            graphdb.createVertex({ id: 0 }, function(err, rootNode) {
                assert(!err, err);

                graphdb.createVertex({ name: "first node" }, function(err, childNode) {
                    assert(!err, err);
                    
                    if (graphdb.server.manager.serverProtocolVersion < 14){
                        var sets = { label: "parent_of" };
                    } else {
                        //label property is reserved, setting an arbitrary property to avoid creating a lightweight edge
                        sets = { name: "edge 1" };
                    }

                    graphdb.createEdge(rootNode, childNode, sets, { "class": "parent_of" }, function(err, edge) {
                        assert(!err, err);

                        if(server.manager.serverProtocolVersion < 14) {
                            assert.equal(rootNode["out"][0], edge["@rid"]);
                            assert.equal(childNode["in"][0], edge["@rid"]);
                        } else {
                            assert.equal(rootNode["out_parent_of"], edge["@rid"]);
                            assert.equal(childNode["in_parent_of"], edge["@rid"]);
                        }
                        
                        assert.equal(rootNode["@rid"], edge["out"]);
                        assert.equal(childNode["@rid"], edge["in"]);

                        if (graphdb.server.manager.serverProtocolVersion < 14){
                            var sets = { label: "child_of" };
                        } else {
                            //label property is reserved, setting an arbitrary property to avoid creating a lightweight edge
                            sets = { name: "edge 2" };
                        }

                        graphdb.createEdge(childNode, rootNode, sets, { "class": "child_of" }, function(err, edge) {
                            assert(!err, err);
                            
                            if (graphdb.server.manager.serverProtocolVersion < 14){
                                var sets = { };
                            } else {
                                //label property is reserved, setting an arbitrary property to avoid creating a lightweight edge
                                sets = { name: "edge 3" };
                            }
                            
                            graphdb.createEdge(childNode["@rid"], rootNode["@rid"], sets, function(err, edge) {
                                assert(!err, err);
                                
                                if(server.manager.serverProtocolVersion < 14) {
                                    childNode["out"].push(edge["@rid"]);
                                    rootNode["in"].push(edge["@rid"]);
                                } else {
                                    childNode["out_"] = edge["@rid"];
                                    rootNode["in_"] = edge["@rid"];
                                }
                                
                                callback(rootNode, childNode);
                            });
                        });

                    });
                });
            });
        });
    });
}

graphdb.open(function(err) {

    assert(!err, "Error while opening the database: " + err);

    if (server.manager.serverProtocolVersion < 14) {
        assert.equal("OGraphVertex", graphdb.getClassByName("V").name);
        assert.equal("OGraphEdge", graphdb.getClassByName("E").name);
    } else {
        assert.equal("V", graphdb.getClassByName("V").name);
        assert.equal("E", graphdb.getClassByName("E").name);
    }

    createVertexes(graphdb, function(rootNode, childNode) {
        graphdb.getOutEdges(rootNode, function(err, outEdges) {
            assert(!err);

            assert.equal(1, outEdges.length);

            graphdb.getInVertex(outEdges[0], function(err, vertex) {
                assert(!err);

                assert.equal(childNode["@rid"], vertex["@rid"]);

                graphdb.getInEdges(childNode, function(err, inEdges) {
                    assert(!err);

                    assert.equal(1, inEdges.length);

                    graphdb.getOutVertex(inEdges[0], function(err, vertex) {
                        assert(!err);

                        assert.equal(rootNode["@rid"], vertex["@rid"]);

                        graphdb.getOutEdges(childNode, function(err, outEdges) {
                            assert(!err);

                            assert.equal(2, outEdges.length);

                            graphdb.getOutEdges(childNode, "child_of", function(err, outEdges) {
                                assert(!err);
                                assert.equal(1, outEdges.length);

                                graphdb.fromVertex(childNode).outVertexes("child_of", function(err, vertexes) {
                                    assert(!err);

                                    assert.equal(1, vertexes.length);

                                    assert.equal(rootNode["@rid"], vertexes[0]["@rid"]);

                                    graphdb.fromVertex(childNode).outVertexes(function(err, vertexes) {
                                        assert(!err);

                                        assert.equal(2, vertexes.length);

                                        graphdb.fromVertex(childNode).inVertexes(function(err, vertexes) {
                                            assert(!err);

                                            assert.equal(1, vertexes.length);
                                            assert.equal(rootNode["@rid"], vertexes[0]["@rid"]);

                                            graphdb.close();
                                        });
                                    });
                                });
                            });
                        });
                    });
                });
            });
        });
    });
});