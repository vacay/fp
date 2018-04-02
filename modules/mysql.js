/* global require, exports */

var fs = require('fs');
var mysql = require('mysql');
var uuid = require('uuid');
var config = require('config-fp');

//var client = mysql.createConnection(config.fp_store);
var pool = mysql.createPool(config.fp_store);

pool.on('error', function(err) {
    console.log(err.code); // 'ER_BAD_DB_ERROR'
});

function writeCodesToFile(filename, fp, trackID, callback) {
    var i = 0;
    var keepWriting = function() {
	var success = true;
	while (success && i < fp.codes.length) {
	    success = file.write(fp.codes[i]+'\t'+fp.times[i]+'\t'+trackID+'\n');
	    i++;
	}
	if (i === fp.codes.length)
	    file.end();
    };
    
    var file = fs.createWriteStream(filename);
    file.on('drain', keepWriting);
    file.on('error', callback);
    file.on('close', callback);
    
    keepWriting();
}

function fpQuery(fp, rows, callback) {
    var fpCodesStr = fp.codes.join(',');
    
    // Get the top N matching tracks sorted by score (number of matched codes)
    var sql = 'SELECT track_id,COUNT(track_id) AS score ' +
	    'FROM codes ' +
	    'WHERE code IN (' + fpCodesStr + ') ' +
	    'GROUP BY track_id ' +
	    'ORDER BY score DESC ' +
	    'LIMIT ' + rows;

    pool.getConnection(function(err, connection) {
	if (err) {
	    callback(err, null);
	    return;
	}

	connection.query(sql, [], function(err, matches) {
	    if (err) {
		callback(err, null);
		connection.release();
		return;
	    }
	    
	    if (!matches || !matches.length) {
		callback(null, []);
		connection.release();
		return;
	    }

	    var trackIDs = new Array(matches.length);
	    var trackIDMap = {};
	    for (var i = 0; i < matches.length; i++) {
		var trackID = matches[i].track_id;
		trackIDs[i] = trackID;
		trackIDMap[trackID] = i;
	    }
	    var trackIDsStr = trackIDs.join('","');
	    
	    // Get all of the matching codes and their offsets for the top N matching
	    // tracks
	    sql = 'SELECT code,time,track_id ' +
		'FROM codes ' +
		'WHERE code IN (' + fpCodesStr + ') ' +
		'AND track_id IN ("' + trackIDsStr + '")';

	    connection.query(sql, [], function(err, codeMatches) {
		if (err) {
		    callback(err, null);
		    connection.release();
		    return;
		}

		for (var i = 0; i < codeMatches.length; i++) {
		    var codeMatch = codeMatches[i];
		    var idx = trackIDMap[codeMatch.track_id];
		    if (idx === undefined) continue;

		    var match = matches[idx];
		    if (!match.codes) {
			match.codes = [];
			match.times = [];
		    }
		    match.codes.push(codeMatch.code);
		    match.times.push(codeMatch.time);
		}

		callback(null, matches);
		connection.release();
	    });
	});
    });
}

function getTrack(trackID, callback) {

    var sql = 'SELECT tracks.* ' +
	    'FROM tracks ' +
	    'WHERE tracks.id=? ';

    pool.getConnection(function(err, connection) {
	if (err) {
	    callback(err, null);
	    return;
	}

	connection.query(sql, [trackID], function(err, tracks) {
	    if (err) {
		callback(err, null);
	    } else if (tracks.length === 1) {
		callback(null, tracks[0]);
	    } else {
		callback(null, null);
	    }

	    connection.release();
	});
    });
}

function addTrack(fp, callback) {

    var length = fp.length;
    if (typeof length === 'string') length = parseInt(length, 10);
    
    if (isNaN(length) || !length) {
	callback('Attempted to add track with invalid duration "' + length + '"', null);
	return;
    }

    if (!fp.codever) {
	callback ('Attempted to add track with missing code version (codever field)', null);
	return;
    }

    var sql = 'INSERT INTO tracks ' +
	    '(codever,name,length,import_date) ' +
	    'VALUES (?,?,?,NOW())';

    pool.getConnection(function(err, connection) {
	if (err) {
	    callback(err, null);
	    return;
	}

	connection.query(sql, [fp.codever, fp.track, length], function(err, info) {

	    if (err) {
		callback(err, null);
		connection.release();
		return;
	    }

	    if (info.affectedRows !== 1) {
		callback('Track insert failed', null);
		connection.release();
		return;
	    }
	
	    var trackID = info.insertId;
	
	    if (config.useLoadDataInfile) {
		// Write out the codes to a file for bulk insertion into MySQL
		var file = config.tmp + '/' + uuid.v4() + '.csv';
		writeCodesToFile(file, fp, trackID, function(err) {
		    if (err) {
			callback(err, null);
			connection.release();
			return;
		    }
		
		    // Bulk insert the codes
		    sql = 'LOAD DATA INFILE ? IGNORE INTO TABLE codes';
		    connection.query(sql, [file], function(err) {
			// Remove the temporary file
			fs.unlink(file, function(err2) {
			    if (!err) err = err2;
			    callback(err, trackID);
			    connection.release();
			});
		    });
		});
	    } else {
		var i=0;
		var valueList=[];
		sql = 'INSERT IGNORE INTO codes ' +
		    '(code, time, track_id) ' +
		    'VALUES ';
	    
		while (i < fp.codes.length) {
		    valueList.push('('+fp.codes[i]+','+fp.times[i]+','+trackID+')');
		    i++;
		}
	    
		sql = sql + valueList.join(',');
		connection.query(sql, function(err) {
		    callback(err, trackID);
		    connection.release();
		});
	    }
	});
    });
}

function updateTrack(trackID, name, callback) {
    var sql = 'UPDATE tracks SET name=? WHERE id=?';

    pool.getConnection(function(err, connection) {

	if (err) {
	    callback(err, null);
	    return;
	}

	connection.query(sql, [name, trackID], function(err, info) {

	    if (err) {
		callback(err, null);
	    } else {
		callback(null, info.affectedRows === 1 ? true : false);
	    }

	    connection.release();
	});
    });
}

function deleteTrack(trackID, callback) {

    var sql = 'DELETE FROM tracks WHERE id=?';

    pool.getConnection(function(err, connection) {
	if (err) {
	    callback(err, null);
	    return;
	}

	connection.query(sql, [trackID], function(err, info) {

	    if (err) {
		callback(err, null);
		return;
	    } else if (info.affectedRows !== 1) {
		callback('AffectedRows was not equal to one when deleting', null);
		return;
	    } else {
		callback(null, trackID);
	    }

	    connection.release();
	});
    });
}

function disconnect(callback) {
    pool.end(callback);
}

exports.fpQuery = fpQuery;
exports.getTrack = getTrack;
exports.addTrack = addTrack;
exports.updateTrack = updateTrack;
exports.deleteTrack = deleteTrack;
exports.disconnect = disconnect;
