/* global require, module, Buffer */

var zlib = require('zlib'),
    config = require('config-fp'),
    log = require('log')(config.log),
    database = require('./mysql'),
    Mutex = require('./mutex');

// Constants
var SECONDS_TO_TIMESTAMP = 43.45,
    MAX_ROWS = 30,
    MIN_MATCH_PERCENT = 0.9,
    MATCH_SLOP = 2,
    CODE_THRESHOLD = 10;

var gMutex = Mutex.getMutex();


/**
 * Takes an uncompressed code string consisting of zero-padded fixed-width
 * sorted hex integers and converts it to the standard code string.
 */
function inflateCodeString(buf) {
    // 5 hex bytes for hash, 5 hex bytes for time (40 bits per tuple)
    var count = Math.floor(buf.length / 5);
    var endTimestamps = count / 2;
    var i;
    var codes = new Array(count / 2);
    var times = new Array(count / 2);
    for (i = 0; i < endTimestamps; i++) {
	times[i] = parseInt(buf.toString('ascii', i * 5, i * 5 + 5), 16);
    }
    for (i = endTimestamps; i < count; i++) {
	codes[i - endTimestamps] = parseInt(buf.toString('ascii', i * 5, i * 5 + 5), 16);
    }
    // Sanity check
    for (i = 0; i < codes.length; i++) {
	if (isNaN(codes[i]) || isNaN(times[i])) {
	    log.error('Failed to parse code/time index ' + i);
	    return {
		codes: [],
		times: []
	    };
	}
    }
    return {
	codes: codes,
	times: times
    };
}

/**
 * Takes a base64 encoded representation of a zlib-compressed code string
 * and passes a fingerprint object to the callback.
 */
function decodeCodeString(codeStr, callback) {
    // Fix url-safe characters
    codeStr = codeStr.replace(/-/g, '+').replace(/_/g, '/');
    // Expand the base64 data into a binary buffer
    var compressed = new Buffer(codeStr, 'base64');
    // Decompress the binary buffer into ascii hex codes
    zlib.inflate(compressed, function (err, uncompressed) {

	if (err) {
	    callback('[decode code string] [inflate] ' + err, null);
	    return;
	}

	// Convert the ascii hex codes into codes and time offsets
	var fp = inflateCodeString(uncompressed);
	log.debug('Inflated ' + codeStr.length + ' byte code string into ' + fp.codes.length + ' codes');
	callback(null, fp);
    });
}

/**
 * Clamp this fingerprint to a maximum N seconds worth of codes.
 */
function cutFPLength(fp, maxSeconds) {
    if (!maxSeconds) maxSeconds = 60;
    var newFP = {};
    for (var key in fp) {
	if (fp.hasOwnProperty(key)) newFP[key] = fp[key];
    }
    var firstTimestamp = fp.times[0];
    var sixtySeconds = maxSeconds * SECONDS_TO_TIMESTAMP + firstTimestamp;
    for (var i = 0; i < fp.times.length; i++) {
	if (fp.times[i] > sixtySeconds) {
	    log.debug('Clamping ' + fp.codes.length + ' codes to ' + i + ' codes');
	    newFP.codes = fp.codes.slice(0, i);
	    newFP.times = fp.times.slice(0, i);
	    return newFP;
	}
    }
    newFP.codes = fp.codes.slice(0);
    newFP.times = fp.times.slice(0);
    return newFP;
}

/**
 * Build a mapping from each code in the given fingerprint to an array of time
 * offsets where that code appears, with the slop factor accounted for in the 
 * time offsets. Used to speed up getActualScore() calculation.
 */
function getCodesToTimes(match, slop) {
    var codesToTimes = {};
    
    for (var i = 0; i < match.codes.length; i++) {
	var code = match.codes[i];
	var time = Math.floor(match.times[i] / slop) * slop;
	
	if (codesToTimes[code] === undefined)
	    codesToTimes[code] = [];
	codesToTimes[code].push(time);
    }
    
    return codesToTimes;
}

/**
 * Computes the actual match score for a track by taking time offsets into
 * account.
 */
function getActualScore(fp, match, threshold, slop) {
    
    if (match.codes.length < threshold)
	return 0;
    
    var timeDiffs = {};
    var i, j;
    
    var matchCodesToTimes = getCodesToTimes(match, slop);
    
    // Iterate over each {code,time} tuple in the query
    for (i = 0; i < fp.codes.length; i++) {
	var code = fp.codes[i];
	var time = Math.floor(fp.times[i] / slop) * slop;

	var matchTimes = matchCodesToTimes[code];
	if (matchTimes) {
	    for (j = 0; j < matchTimes.length; j++) {
		var dist = Math.abs(time - matchTimes[j]);

		// Increment the histogram bucket for this distance
		if (timeDiffs[dist] === undefined)
		    timeDiffs[dist] = 0;
		timeDiffs[dist]++;
	    }
	}
    }

    match.histogram = timeDiffs;
    
    // Convert the histogram into an array, sort it, and sum the top two
    // frequencies to compute the adjusted score
    var keys = Object.keys(timeDiffs);
    var array = new Array(keys.length);
    for (i = 0; i < keys.length; i++)
	array[i] = [ keys[i], timeDiffs[keys[i]] ];
    array.sort(function(a, b) { return b[1] - a[1]; });
    
    if (array.length > 1)
	return array[0][1] + array[1][1];
    else if (array.length === 1)
	return array[0][1];
    return 0;
}

/**
 * Attach track metadata to a query match.
 */
function getTrackMetadata(match, allMatches, status, callback) {
    database.getTrack(match.track_id, function(err, track) {

	if (err) {
	    callback(err, null);
	    return;
	}

	if (!track) {
	    callback('Track ' + match.track_id + ' went missing', null);
	    return;
	}
	
	match.track = track.name;
	match.length = track.length;
	match.import_date = track.import_date;

	var result = {
	    success: true,
	    status: status,
	    match: match
	};
	
	callback(null, result, allMatches);
    });
}

/**
 * Finds the closest matching track, if any, to a given fingerprint.
 */
function bestMatchForQuery(fp, threshold, callback) {
    fp = cutFPLength(fp);
    
    if (!fp.codes.length) {
	callback('No valid fingerprint codes specified', null);
	return;
    }
    
    log.debug('Starting query with ' + fp.codes.length + ' codes');
    
    database.fpQuery(fp, MAX_ROWS, function(err, matches) {
	if (err) {
	    callback(err, null);
	    return;
	}
	
	if (!matches || !matches.length) {
	    log.debug('No matched tracks');
	    callback(null, { status: 'NO_RESULTS' });
	    return;
	}
	
	log.debug('Matched ' + matches.length + ' tracks, top code overlap is ' + matches[0].score);
	
	// If the best result matched fewer codes than our percentage threshold,
	// report no results
	if (matches[0].score < fp.codes.length * MIN_MATCH_PERCENT) {
	    log.debug('Multiple bad match score: ' + matches[0].score + ' found ' + fp.codes.length + ' codes');
	    callback(null, { status: 'MULTIPLE_BAD_HISTOGRAM_MATCH' });
	    return;
	}

	// Compute more accurate scores for each track by taking time offsets into
	// account
	var newMatches = [];
	for (var i = 0; i < matches.length; i++) {
	    var match = matches[i];
	    match.ascore = getActualScore(fp, match, threshold, MATCH_SLOP);
	    if (match.ascore && match.ascore >= fp.codes.length * MIN_MATCH_PERCENT)
		newMatches.push(match);
	}
	matches = newMatches;
	
	if (!matches.length) {
	    log.debug('No matched tracks after score adjustment');
	    callback(null, { status: 'NO_RESULTS_HISTOGRAM_DECREASED' });
	    return;
	}
	
	// Sort the matches based on actual score
	matches.sort(function(a, b) { return b.ascore - a.ascore; });
	
	// If we only had one track match, just use the threshold to determine if
	// the match is good enough
	if (matches.length === 1) {

	    if (matches[0].ascore / fp.codes.length >= MIN_MATCH_PERCENT) {
		// Fetch metadata for the single match
		log.debug('Single good match with actual score ' + matches[0].ascore + '/' + fp.codes.length);
		getTrackMetadata(matches[0], matches, 'SINGLE_GOOD_MATCH_HISTOGRAM_DECREASED', callback);
	    } else {
		log.debug('Single bad match with actual score ' + matches[0].ascore + '/' + fp.codes.length);
		callback(null, { status: 'SINGLE_BAD_MATCH' });
	    }

	    return;
	}
	
	var origTopScore = matches[0].ascore;
	
	// Sort by the new adjusted score
	matches.sort(function(a, b) { return b.ascore - a.score; });
	
	var topMatch = matches[0];
	var newTopScore = topMatch.ascore;
	
	log.debug('Actual top score is ' + newTopScore + ', next score is ' + matches[1].ascore);
	
	// If the best result actually matched fewer codes than our percentage
	// threshold, report no results
	if (newTopScore < fp.codes.length * MIN_MATCH_PERCENT) {
	    log.debug('Multiple bad match score (percentage): ' + newTopScore + ' found ' + fp.codes.length + ' codes');
	    callback(null, { status: 'MULTIPLE_BAD_HISTOGRAM_MATCH' });
	    return;
	}

	// If the actual score was not close enough, then no match
	if (newTopScore <= origTopScore / 2) {
	    log.debug('Multiple bad match score (not close enough): ' + newTopScore + ' found ' + fp.codes.length + ' codes and orig score was ' + origTopScore);
	    callback(null, { status: 'MULTIPLE_BAD_HISTOGRAM_MATCH' });
	    return;
	}

	// If the difference in actual scores between the first and second matches
	// is not significant enough, then no match 
	if (newTopScore - matches[1].ascore < newTopScore / 2) {
	    log.debug('Multiple bad match score (second match): ' + newTopScore + ' second match score ' + matches[1].ascore);
	    callback(null, { status: 'MULTIPLE_BAD_HISTOGRAM_MATCH' });
	    return;
	}
	
	// Fetch metadata for the top track
	getTrackMetadata(topMatch, matches, 'MULTIPLE_GOOD_MATCH_HISTOGRAM_DECREASED', callback);
    });
}

/**
 * Takes a vitamin fingerprint (includes codes and time offsets plus any
 * available metadata), adds it to the database and returns a vitamin_id
 */
function ingest(fp, callback) {
    
    if (!fp.codes.length || typeof fp.length !== 'number' || !fp.codever) {
	callback('[ingest] Missing required vitamin fields', null);
	return;
    }

    if (!fp.codes.length) {
	callback('[ingest] Missing "codes" array', null);
	return;
    }

    if (typeof fp.length !== 'number') {
	callback('[ingest] Missing or invalid "length" field', null);
	return;
    }

    if (!fp.codever) {
	callback('[ingest] Missing or invalid "version" field', null);
	return;
    }

    // Acquire a lock while modifying the database
    gMutex.lock(function() {
	
	function createTrack() {
	    database.addTrack(fp, function(err, trackID) {
		if (err) {
		    gMutex.release();
		    callback(err, null);
		    return;
		}
		
		// Success
		log.info('Created track ' + trackID + ' ("' + fp.track + '")');
		gMutex.release();
		callback(null, { track_id: trackID, track: fp.track });
	    });
	}

	// Check if this track already exists in the database
	bestMatchForQuery(fp, CODE_THRESHOLD, function(err, res) {
	    if (err) {
		gMutex.release();
		callback('Query failed: ' + err, null);
		return;
	    }
	    
	    if (res.success) {
		var match = res.match;
		log.info('Found existing match with status ' + res.status +
			 ', track ' + match.track_id + ' ("' + match.track + '")');
		
		if (!match.track && fp.track) {
		    // Existing track is unnamed but we have a name now. Update the track
		    log.debug('Updating track name to "' + fp.track + '"');
		    database.updateTrack(match.track_id, fp.track, function(err) {

			if (err) {
			    gMutex.release();
			    callback(err, null);
			    return;
			}

			match.track = fp.track;
			gMutex.release();
			callback(null, { track_id: match.track_id, track: fp.track });
		    });

		} else {
		    log.debug('Skipping track name update');
		    gMutex.release();
		    callback(null, { track_id: match.track_id, track: fp.track });
		}

	    } else {
		// Track does not exist in the database yet
		log.debug('Track does not exist in the database yet, status ' + res.status);
		createTrack();
	    }
	});
    });
    
}

module.exports = {
    decodeCodeString: decodeCodeString,
    ingest: ingest
};
