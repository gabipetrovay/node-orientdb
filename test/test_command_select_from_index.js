var assert = require("assert");
var _ = require("lodash");

var db = require("./setup_db.js").db;

db.open(function(err) {

    assert(!err, "Error while opening the database: " + err);

    prepareDatabase(function(err) {
        assert(!err, err);

        db.save({name: "john", surname: "doe", "@class": "ClassWithIndex" }, function(err) {
            assert(!err, err);

            db.command("SELECT FROM index:ClassWithIndex.surname where key = 'non_existent'", function(err, results) {
                assert(!err, err);

                assert.equal(0, results.length);

                db.command("SELECT FROM index:ClassWithIndex.surname where key = 'doe'", function(err, results) {
                    assert(!err, err);

                    assert.equal(1, results.length);

                    db.save({name: "john", surname: "doe", "@class": "ClassWithIndex" }, function(err) {
                        assert(err, "unique index violeted?!?");

                        db.save({name: "john m", surname: "smith", "@class": "ClassWithIndex" }, function(err) {
                            assert(!err, err);

                            db.command("SELECT FROM index:ClassWithIndex.name where key containstext 'john'", { fetchPlan: "*:-1" }, function(err, results) {
                                assert(!err, err);

                                assert.equal(2, results.length);
								
								var rowIndices;
								
								if (results[0].rid.name == "john")
									rowIndices = [0,1];
								else
									rowIndices = [1,0];

                                assert.equal("john", results[rowIndices[0]].rid.name);
                                assert.equal("doe", results[rowIndices[0]].rid.surname);
                                assert.equal("john m", results[rowIndices[1]].rid.name);
                                assert.equal("smith", results[rowIndices[1]].rid.surname);

                                db.command("DELETE FROM ClassWithIndex", function(err) {
                                    assert(!err, err);

                                    db.dropClass("ClassWithIndex", function(err) {
                                        assert(!err, err);

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
});

function prepareDatabase(callback) {
    db.createClass("ClassWithIndex", function(err) {
        if (err) return callback(err);

        db.command("CREATE PROPERTY ClassWithIndex.name STRING", function(err) {
            if (err) return callback(err);

            db.command("CREATE INDEX ClassWithIndex.name FULLTEXT", function(err) {
                if (err) return callback(err);

                db.command("CREATE PROPERTY ClassWithIndex.surname STRING", function(err) {
                    if (err) return callback(err);

                    db.command("CREATE INDEX ClassWithIndex.surname UNIQUE", function(err) {
                        if (err) return callback(err);

                        callback();
                    });
                });
            });
        });
    });
}