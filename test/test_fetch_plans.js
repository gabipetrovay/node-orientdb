var assert = require("assert");
var _ = require("lodash");

var orient = require("../lib/orientdb"),
    GraphDb = orient.GraphDb,
    Server = orient.Server;

var serverConfig = require("../config/test/serverConfig");
var dbConfig = require("../config/test/dbConfig");

var server = new Server(serverConfig);
var db = new GraphDb("temp", server, dbConfig);


db.open(function(err) {
    assert(!err, err);

    prepareDatabase(function() {
        prepareDatabase(function(err, first, second, third, first_to_second, second_to_third, third_to_first) {
            assert(!err, err);

            db.loadRecord(first["@rid"], { fetchPlan: "*:1" }, function(err, vertex) {
                assert(!err, err);

                assert.equal("first", vertex.name);
                
                if (db.server.manager.serverProtocolVersion >= 14) {
                    assert.equal("first_to_second", vertex.out_.name);
                    assert.equal(first["@rid"], vertex.out_.out);
                    assert.equal(second["@rid"], vertex.out_.in);

                    assert.equal(third["@rid"], vertex.in_.out);
                    assert.equal(first["@rid"], vertex.in_.in);
                } else {
                    assert.equal("first_to_second", vertex.out[0].name);
                    assert.equal(first["@rid"], vertex.out[0].out);
                    assert.equal(second["@rid"], vertex.out[0].in);

                    assert.equal(third["@rid"], vertex.in[0].out);
                    assert.equal(first["@rid"], vertex.in[0].in);
                }

                db.loadRecord(first["@rid"], { fetchPlan: "*:2" }, function(err, vertex) {
                    assert(!err, err);

                    assert.equal("first", vertex.name);

                    if (db.server.manager.serverProtocolVersion >= 14) {
                        assert.equal("first_to_second", vertex.out_.name);
                        assert.equal(first["@rid"], vertex.out_.out);
                        assert.equal("second", vertex.out_.in.name);
                        assert.equal(first_to_second["@rid"], vertex.out_.in.in_);
                        assert.equal(second_to_third["@rid"], vertex.out_.in.out_);

                        assert.equal("third_to_first", vertex.in_.name);
                        assert.equal(first["@rid"], vertex.in_.in);
                        assert.equal("third", vertex.in_.out.name);
                        assert.equal(second_to_third["@rid"], vertex.in_.out.in_);
                        assert.equal(third_to_first["@rid"], vertex.in_.out.out_);
                    } else {
                        assert.equal("first_to_second", vertex.out[0].name);
                        assert.equal(first["@rid"], vertex.out[0].out);
                        assert.equal("second", vertex.out[0].in.name);
                        assert.equal(first_to_second["@rid"], vertex.out[0].in.in[0]);
                        assert.equal(second_to_third["@rid"], vertex.out[0].in.out[0]);

                        assert.equal("third_to_first", vertex.in[0].name);
                        assert.equal(first["@rid"], vertex.in[0].in);
                        assert.equal("third", vertex.in[0].out.name);
                        assert.equal(second_to_third["@rid"], vertex.in[0].out.in[0]);
                        assert.equal(third_to_first["@rid"], vertex.in[0].out.out[0]);
                    }

                    db.loadRecord(first["@rid"], { fetchPlan: "*:-1" }, function(err, vertex) {
                        assert(!err, err);

                        assertVertexHierarchyIsComplete(vertex, first, first_to_second, second, second_to_third, third, third_to_first);

                        db.command("select from " + first["@rid"], { fetchPlan: "*:-1" }, function(err, results) {
                            assert(!err, err);

                            assert.equal(1, results.length);

                            var vertex = results[0];

                            assertVertexHierarchyIsComplete(vertex, first, first_to_second, second, second_to_third, third, third_to_first);

                            db.command("select from V where name = 'first'", { fetchPlan: "*:-1" }, function(err, results) {
                                assert(!err, err);

                                assert(results.length > 1);

                                var vertex = _.last(results);

                                assertVertexHierarchyIsComplete(vertex, first, first_to_second, second, second_to_third, third, third_to_first);

                                db.command("insert into V (name) values ('other')", { fetchPlan: "*:-1" }, function(err, results) {
                                    assert(err, "Cannot execute async queries with other than selects");

                                    db.close();
                                });
                            });
                        });
                    });
                });
            });
        });
    });
});

function assertVertexHierarchyIsComplete(vertex, first, first_to_second, second, second_to_third, third, third_to_first) {
    assert.equal("first", vertex.name);

    if (db.server.manager.serverProtocolVersion >= 14) {
        assert.equal("first_to_second", vertex.out_.name);
        assert.equal("second", vertex.out_.in.name);
        assert.equal(first_to_second["@rid"], vertex.out_.in.in_);
        assert.equal("second_to_third", vertex.out_.in.out_.name);
        assert.equal(second["@rid"], vertex.out_.in.out_.out);
        assert.equal("third", vertex.out_.in.out_.in.name);
        assert.equal(second_to_third["@rid"], vertex.out_.in.out_.in.in_);
        assert.equal("third_to_first", vertex.out_.in.out_.in.out_.name);
        assert.equal(third["@rid"], vertex.out_.in.out_.in.out_.out);
        assert.equal(first["@rid"], vertex.out_.in.out_.in.out_.in);

        assert.equal("third_to_first", vertex.in_.name);
        assert.equal(first["@rid"], vertex.in_.in);
        assert.equal("third", vertex.in_.out.name);
        assert.equal("second_to_third", vertex.in_.out.in_.name);
        assert.equal(third["@rid"], vertex.in_.out.in_.in);
        assert.equal("second", vertex.in_.out.in_.out.name);
        assert.equal(second_to_third["@rid"], vertex.in_.out.in_.out.out_);
        assert.equal("first_to_second", vertex.in_.out.in_.out.in_.name);
        assert.equal(second["@rid"], vertex.in_.out.in_.out.in_.in);
        assert.equal(first["@rid"], vertex.in_.out.in_.out.in_.out);
        assert.equal(third_to_first["@rid"], vertex.in_.out.out_);
    } else {
        assert.equal("first_to_second", vertex.out[0].name);
        assert.equal("second", vertex.out[0].in.name);
        assert.equal(first_to_second["@rid"], vertex.out[0].in.in[0]);
        assert.equal("second_to_third", vertex.out[0].in.out[0].name);
        assert.equal(second["@rid"], vertex.out[0].in.out[0].out);
        assert.equal("third", vertex.out[0].in.out[0].in.name);
        assert.equal(second_to_third["@rid"], vertex.out[0].in.out[0].in.in[0]);
        assert.equal("third_to_first", vertex.out[0].in.out[0].in.out[0].name);
        assert.equal(third["@rid"], vertex.out[0].in.out[0].in.out[0].out);
        assert.equal(first["@rid"], vertex.out[0].in.out[0].in.out[0].in);

        assert.equal("third_to_first", vertex.in[0].name);
        assert.equal(first["@rid"], vertex.in[0].in);
        assert.equal("third", vertex.in[0].out.name);
        assert.equal("second_to_third", vertex.in[0].out.in[0].name);
        assert.equal(third["@rid"], vertex.in[0].out.in[0].in);
        assert.equal("second", vertex.in[0].out.in[0].out.name);
        assert.equal(second_to_third["@rid"], vertex.in[0].out.in[0].out.out[0]);
        assert.equal("first_to_second", vertex.in[0].out.in[0].out.in[0].name);
        assert.equal(second["@rid"], vertex.in[0].out.in[0].out.in[0].in);
        assert.equal(first["@rid"], vertex.in[0].out.in[0].out.in[0].out);
        assert.equal(third_to_first["@rid"], vertex.in[0].out.out[0]);
    }
}

function prepareDatabase(callback) {
    db.createVertex({ name: "first" }, function(err, first) {
        if (err) { return callback(err); }

        db.createVertex({ name: "second" }, function(err, second) {
            if (err) { return callback(err); }

            db.createVertex({ name: "third" }, function(err, third) {
                if (err) { return callback(err); }

                db.createEdge(first, second, { name: "first_to_second" }, function(err, first_to_second) {
                    if (err) { return callback(err); }

                    db.createEdge(second, third, { name: "second_to_third" }, function(err, second_to_third) {
                        if (err) { return callback(err); }

                        db.createEdge(third, first, { name: "third_to_first" }, function(err, third_to_first) {
                            if (err) { return callback(err); }

                            callback(null, first, second, third, first_to_second, second_to_third, third_to_first);
                        });
                    })
                });
            });
        });
    });
}
