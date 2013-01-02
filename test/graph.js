var assert = require("assert");
var Step = require("step");

var orient = require("../lib/orientdb"),
    GraphDb = orient.GraphDb,
    Server = orient.Server;

var serverConfig = require("../config/test/serverConfig");
var dbConfig = require("../config/test/dbConfig");

var server = new Server(serverConfig);
var graphdb = new GraphDb("temp", server, dbConfig);

function createVertexes(graphdb, callback) {
    var rootNode, childNode;
    Step(
        function createRootVertex() {
            graphdb.createVertex({ id: 0 }, this);
        },
        function createFirstVertex(err, root) {
            assert(!err, err);

            rootNode = root;

            graphdb.createVertex({ name: "first node" }, this);
        },
        function createEdgeFromRootToFirstVertex(err, child) {
            assert(!err, err);

            childNode = child;

            graphdb.createEdge(rootNode, childNode, this);
        },
        function createEdgeFromFirstVertexToRoot(err, edge) {
            assert(!err, err);

            assert.equal(rootNode["out"][0], edge["@rid"]);
            assert.equal(childNode["in"][0], edge["@rid"]);

            assert.equal(rootNode["@rid"], edge["out"]);
            assert.equal(childNode["@rid"], edge["in"]);

            graphdb.createEdge(childNode, rootNode, { label: "child_of" }, this);
        },
        function createEdgeByRID(err, edge) {
            assert(!err, err);

            graphdb.createEdge(childNode["@rid"], rootNode["@rid"], this);
        },
        function pushEdgeRIDIntoVertexes(err, edge) {
            assert(!err, err);

            childNode["out"].push(edge["@rid"]);
            rootNode["in"].push(edge["@rid"]);

            callback(rootNode, childNode);
        }
    );
}

graphdb.open(function(err) {

    assert(!err, "Error while opening the database: " + err);

    assert.equal("OGraphVertex", graphdb.getClassByName("OGraphVertex").name);
    assert.equal("OGraphVertex", graphdb.getClassByName("V").name);
    assert.equal("OGraphEdge", graphdb.getClassByName("OGraphEdge").name);
    assert.equal("OGraphEdge", graphdb.getClassByName("E").name);

    var rootNode, childNode;
    Step(
        function() {
            createVertexes(graphdb, this);
        },
        function(root, child) {
            rootNode = root;
            childNode = child;
            graphdb.getOutEdges(rootNode, this);
        },
        function(err, outEdges) {
            assert(!err);

            assert.equal(1, outEdges.length);

            graphdb.getInVertex(outEdges[0], this);
        },
        function(err, vertex) {
            assert(!err, err);

            assert.equal(childNode["@rid"], vertex["@rid"]);

            graphdb.getInEdges(childNode, this);
        },
        function(err, inEdges) {
            assert(!err);

            assert.equal(1, inEdges.length);

            graphdb.getOutVertex(inEdges[0], this);
        },
        function(err, vertex) {
            assert(!err);

            assert.equal(rootNode["@rid"], vertex["@rid"]);

            graphdb.getOutEdges(childNode, this);
        },
        function(err, outEdges) {
            assert(!err);

            assert.equal(2, outEdges.length);

            graphdb.getOutEdges(childNode, "child_of", this);
        },
        function(err, outEdges) {
            assert(!err);
            assert.equal(1, outEdges.length);

            graphdb.fromVertex(childNode).outVertexes("child_of", this);
        },
        function(err, vertexes) {
            assert(!err);

            assert.equal(1, vertexes.length);

            assert.equal(rootNode["@rid"], vertexes[0]["@rid"]);

            graphdb.fromVertex(childNode).outVertexes(this);
        },
        function(err, vertexes) {
            assert(!err);

            assert.equal(2, vertexes.length);

            graphdb.fromVertex(childNode).inVertexes(this);
        },
        function(err, vertexes) {
            assert(!err);

            assert.equal(1, vertexes.length);
            assert.equal(rootNode["@rid"], vertexes[0]["@rid"]);

            graphdb.close();
        });
});
