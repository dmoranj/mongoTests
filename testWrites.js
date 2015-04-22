var MongoClient = require('mongodb').MongoClient,
    assert = require('assert'),
    uuid = require('node-uuid'),
    async = require('async'),
    apply = async.apply,
    entities = [],
    initialTime,
    globalDb;

var url = 'mongodb://' + process.env['TARGET_DB'] + ':27017/performance?w=0';

function insertDocument(collection, documentGenerator, insertionId, callback) {
  var collection = globalDb.collection(collection);

  collection.save(documentGenerator(), function(err, result) {
    if (err) {
	callback(err);
    } else {
	callback();
    }
  });
}

function readDocument(collection, readId, callback) {
  var collection = globalDb.collection(collection),
      id = selectEntity();
  collection.find({ id: id }).toArray(function(error, docs) {
    if (error) {
      callback(error);
    } else {
      callback();
    }
  });
}

function removeDocument(collection, removeId, callback) {
  var collection = globalDb.collection(collection),
      id = popEntity();

  collection.remove({ id: id[0] }, function(error, n) {
    if (error) {
      callback(error);
    } else {
      callback();
    }
  });
}

function initializeDb(callback) {
  MongoClient.connect(url, function(err, db) {
    assert.equal(null, err);
    console.log("Connected correctly to server");

    globalDb = db;
    callback();
  });
}

function generateEntity() {
  var id = uuid.v4();
  entities.push(id);
  return {id: id, a: uuid.v4(), b: uuid.v4(), c: {d: uuid.v4()} };
}

function popEntity() {
  return entities.splice(Math.random()*entities.length, 1);
}

function selectEntity() {
  return entities[Math.random()*entities.length];
}

function test1(n, callback) {
  console.log('Test1: Writing %d records to the db', n);
  async.times(n , apply(insertDocument, 'testCollection', generateEntity), callback);
}

function test2(n, callback) {
  console.log('Test2: Reading %d records from the db', n);
  async.times(n , apply(readDocument, 'testCollection'), callback);
}

function test3(n, callback) {
  console.log('Test3: Deleting %d records from the db', n);
  async.times(n , apply(removeDocument, 'testCollection'), callback);
}

function prepare(callback) {
  globalDb.dropDatabase(callback);
}

function cleanUp(error) {
  if (error) {
    console.log('There were some errors executing: ' + error);
  } else {
    console.log('Success');
  }

  globalDb.close();
}

function startTimer(name, callback) {
  console.time(name);
  callback();
}

function stopTimer(name, callback) {
  console.timeEnd(name);
  callback();
}

function executeSerial(size) {
  async.series([
    initializeDb,
    prepare,
    apply(startTimer, 'Test1'),
    apply(test1, size),
    apply(stopTimer, 'Test1'),
    apply(startTimer, 'Test2'),
    apply(test2, size),
    apply(stopTimer, 'Test2'),
    apply(startTimer, 'Test3'),
    apply(test3, size),
    apply(stopTimer, 'Test3')

  ], cleanUp);
}

function agent(times, agentId, callback) {
  var count = 0,
      errors = 0,
      functions = [
        apply(insertDocument, 'testCollection', generateEntity, count),
        apply(readDocument, 'testCollection', count),
        apply(readDocument, 'testCollection', count),
        apply(insertDocument, 'testCollection', generateEntity, count),
        apply(removeDocument, 'testCollection', count)	,
        apply(insertDocument, 'testCollection', generateEntity, count)
      ];

  async.whilst(
    function () { return count < times; },
    function (innerCallback) {
        if (count % 200 === 0) {
		console.log('%d Operations on Agent %j on process %d', count, agentId, process.pid);
	}

	functions[Math.floor(Math.random()*functions.length)](function(error) {
		if (!error) {
			count++;
		} else {
			console.log('Ay el error %j' + error); 
			errors++;
		}
		innerCallback();
	});
    },
    function (err) {
        console.log('%j Errors in agent %d', errors, agentId);
        callback(err);
    }
);
}

function executeParallel(agents, size) {
  console.log('%d Agents executing %d operations: %d', agents, size, agents*size);  
  async.series([
    initializeDb,
    prepare,
    apply(startTimer, 'TestRandom'),
    apply(async.times, agents, apply(agent, size)),
    apply(stopTimer, 'TestRandom')
  ], cleanUp);
}

if (process.env['TARGET_DB']) {
	console.log(process.argv.length);
	if (process.argv.length !== 3) {
		console.log('Use:\n\tnode testWrites.sh <serial|parallel>');
	} else {
		if (process.argv[2] === 'serial') {
			executeSerial(5000);			
		} else {
			executeParallel(5, 3000);
		}
	}
} else {
	console.log('Undefined TARGET_DB');
}
