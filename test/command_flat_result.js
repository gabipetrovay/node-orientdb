var assert = require("assert");

var orient = require("../lib/orientdb"),
    Db = orient.Db,
    Server = orient.Server;

var serverConfig = require("../config/test/serverConfig");
var dbConfig = require("../config/test/dbConfig");

var server = new Server(serverConfig);
var db = new Db("temp", server, dbConfig);


var className = "TestFlatResult";

db.open(function(err, result) {

    assert(!err, "Error while opening the database: " + err);

    var originalClassCount = db.classes.length;
    console.log("The database has " + originalClassCount + " classes.");

    db.command("CREATE CLASS " + className, function(err, results) {

        if (err) {
            assert(!err, "Error while executing a CREATE CLASS command: " + (err.message || JSON.stringify(err)));
        }

        db.reload(function(err) {
            assert(!err, err);

            assert.equal(results.length, 1, "The new class count should be returned.");
            assert.equal(db.classes.length, results[0], "The Db object classes has not been updated.");

            console.log("Created class " + className);
            console.log("The database has now " + db.classes.length + " classes.");

            db.command("DROP CLASS " + className, function(err, results) {

                if (err) {
                    assert(!err, "Error while executing a DROP CLASS command: " + (err.message || JSON.stringify(err)));
                }

                assert.equal(results.length, 1, "The result should contain the boolean status of the DROP operation.");
                assert(results[0], "Drop class should be successful, it wasn't")

                console.log("Dropped class " + className);

                db.reload(function(err) {
                    assert(!err, err);

                    assert.equal(originalClassCount, db.classes.length, "The Db object classes has not been updated.");

                    console.log("The database has now " + db.classes.length + " classes.");

                    db.close();
                });
            });
        });
    });
});

