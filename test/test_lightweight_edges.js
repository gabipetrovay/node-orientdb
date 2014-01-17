var assert = require("assert"),
    async = require("async"),
    _ = require("lodash");

var orient = require("../lib/orientdb"),
    GraphDb = orient.GraphDb,
    Server = orient.Server;

var serverConfig = require("../config/test/serverConfig");
var dbConfig = require("../config/test/dbConfig");

var server = new Server(serverConfig);
var graphdb = new GraphDb("temp", server, dbConfig);

var data = {};
async.waterfall([
    function(callback){
        graphdb.open(callback);
    },
    function(callback){
        if (graphdb.server.manager.serverProtocolVersion < 14) {
        	//lightweight threads feature not implemented, so exit waterfall
            return callback("exit");
        }
        
        graphdb.createClass("EdgeType1", "E", callback);
    },
    function(clusterId, callback){
        data.edgeType1_clusterId = clusterId;
        graphdb.createClass("EdgeType2", "E", callback);
    },
    function(clusterId, callback){
        data.edgeType2_clusterId = clusterId;
        graphdb.createVertex({ id: 1 }, callback);
    },
    function(node, callback){
        data.rootNode = node;
        graphdb.createVertex({ name: "child node 1" }, callback);
    },
    function(node, callback){
        data.childNode1 = node;
        graphdb.createVertex({ name: "child node 2" }, callback);
    },
    function(node, callback){
        data.childNode2 = node;
        graphdb.createVertex({ name: "child node 3" }, callback);
    },
    function(node, callback){
        data.childNode3 = node;
        graphdb.createVertex({ name: "child node 4" }, callback);
    },
    function(node, callback){
        data.childNode4 = node;
        graphdb.createVertex({ name: "child node 5" }, callback);
    },
    function(node, callback){
        data.childNode5 = node;
        graphdb.createVertex({ name: "child node 6" }, callback);
    },
    function(node, callback){
        data.childNode6 = node;
        graphdb.createVertex({ name: "child node 7" }, callback);
    },
    function(node, callback){
        data.childNode7 = node;
        graphdb.createEdge(data.rootNode, data.childNode1, callback);
    },
    function(edge, callback){
        data.edge1 = edge;
        graphdb.createEdge(data.rootNode, data.childNode2, {}, {"class": "EdgeType1"}, callback);
    },
    function(edge, callback){
        data.edge2 = edge;
        graphdb.createEdge(data.rootNode, data.childNode3, {}, {"class": "EdgeType2"}, callback);
    },
    function(edge, callback){
        data.edge3 = edge;
        graphdb.createEdge(data.rootNode, data.childNode4, {}, {"class": "EdgeType2"}, callback);
    },
    function(edge, callback){
        data.edge4 = edge;
        graphdb.createEdge(data.rootNode, data.childNode5, {"AProperty":1}, {"class": "EdgeType1"}, callback);
    },
    function(edge, callback){
        data.edge5 = edge;
        graphdb.createEdge(data.rootNode, data.childNode6, {"AProperty":2}, {"class": "EdgeType2"}, callback);
    },
    function(edge, callback){
        data.edge6 = edge;
        graphdb.createEdge(data.rootNode, data.childNode7, {"AProperty":3}, {"class": "EdgeType2"}, callback);
    },
    function(edge, callback){
        data.edge7 = edge;
        graphdb.loadRecord(data.rootNode["@rid"], {fetchPlan: "*:-1", ignoreCache: true }, callback);
    },
    function(node, callback){
        assert.equal(node.out_["@class"], "V");
        assert.equal(node.out_.in_, node["@rid"]);
        //in/out properties are either an object or an array based on how many edges are represented...
        //edges with a custom type are given there own in/out properties on vertexes...
        assert(_.isArray(node.out_EdgeType1));
        assert.equal(node.out_EdgeType1.length, 2);
        //lightweight edges (no properties) link directly to vertex rather than an edge document...
        assert.equal(node.out_EdgeType1[0]["@class"], "V");
        assert.equal(node.out_EdgeType1[0].in_EdgeType1, node["@rid"]);
        assert.equal(node.out_EdgeType1[1]["@class"], "EdgeType1");
        assert.equal(node.out_EdgeType1[1].AProperty, 1);
        assert.equal(node.out_EdgeType1[1].out, node["@rid"]);
        assert(_.isObject(node.out_EdgeType1[1].in));
        assert.equal(node.out_EdgeType1[1].in["@class"], "V");
        assert.equal(node.out_EdgeType1[1].in.in_EdgeType1, node.out_EdgeType1[1]["@rid"]);

        assert.equal(node.out_EdgeType2.length, 4);
        assert.equal(node.out_EdgeType2[0]["@class"], "V");
        assert.equal(node.out_EdgeType2[0].in_EdgeType2, node["@rid"]);
        assert.equal(node.out_EdgeType2[1]["@class"], "V");
        assert.equal(node.out_EdgeType2[1].in_EdgeType2, node["@rid"]);
        assert.equal(node.out_EdgeType2[2]["@class"], "EdgeType2");
        assert.equal(node.out_EdgeType2[2].AProperty, 2);
        assert.equal(node.out_EdgeType2[2].out, node["@rid"]);
        assert(_.isObject(node.out_EdgeType2[2].in));
        assert.equal(node.out_EdgeType2[2].in["@class"], "V");
        assert.equal(node.out_EdgeType2[2].in.in_EdgeType2, node.out_EdgeType2[2]["@rid"]);
        assert.equal(node.out_EdgeType2[3].out, node["@rid"]);
        assert(_.isObject(node.out_EdgeType2[3].in));
        assert.equal(node.out_EdgeType2[3].in["@class"], "V");
        assert.equal(node.out_EdgeType2[3].in.in_EdgeType2, node.out_EdgeType2[3]["@rid"]);
        return callback(null);
    },
    function(callback){
        graphdb.delete(data.edge7, callback);
    },
    function(result, callback){
        graphdb.delete(data.edge6, callback);
    },
    function(result, callback){
        graphdb.delete(data.edge5, callback);
    },
    //edge1 through edge4 are lightweight and will be deleted when their referring nodes are deleted
    function(result, callback){
        graphdb.delete(data.childNode7, callback);
    },
    function(result, callback){
        graphdb.delete(data.childNode6, callback);
    },
    function(result, callback){
        graphdb.delete(data.childNode5, callback);
    },
    function(result, callback){
        graphdb.delete(data.childNode4, callback);
    },
    function(result, callback){
        graphdb.delete(data.childNode3, callback);
    },
    function(result, callback){
        graphdb.delete(data.childNode2, callback);
    },
    function(result, callback){
        graphdb.delete(data.childNode1, callback);
    },
    function(result, callback){
        graphdb.delete(data.rootNode, callback);
    },
    function(result, callback){
        graphdb.dropClass("EdgeType2", callback);
    },
    function(callback){
        graphdb.dropClass("EdgeType1", callback);
    },
    function(callback){
        graphdb.close(callback);
    }],
    function(err, result){
        assert(!err || err==="exit", err);
    
        graphdb.close();
    });
