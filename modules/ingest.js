/* global require, module */

var config = require('config-fp'),
    log = require('log')(config.log),
    fingerprinter = require('./fingerprinter');

var CODE_VER = 4.12;

module.exports = function (data, callback) {
    var code = data.code;
    var codeVer = data.metadata.version;
    var length = parseInt(data.metadata.duration, 10);
    var bitrate = data.metadata.bitrate;
    var track = data.metadata.title;
    var start = new Date();
    
    if (!code) {
	callback('[ingest] Missing "code" field', null);
	return;
    }

    if (!codeVer) {
	callback('[ingest] Missing "version" field', null);
	return;
    }

    if (codeVer !== CODE_VER) {
	callback('[ingest] Version "' + codeVer + '" does not match required version "' + CODE_VER + '"', null);
	return;
    }
    
    if (isNaN(parseInt(length, 10))) {
	callback('[ingest] Missing or invalid "length" field', null);
	return;
    }
    
    fingerprinter.decodeCodeString(code, function(err, fp) {
	if (err || !fp.codes.length) {
	    callback('[fingerprinter] ' + err, null);
	    return;
	}
	
	fp.codever = codeVer;
	fp.length = parseInt(length, 10);
	fp.track = track;

	fingerprinter.ingest(fp, function (err, result) {
	    if (err) {
		callback('[fingerprint] ' + err, null);
		return;
	    }

	    var duration = new Date() - start;
	    log.debug('[ingest] completed in ' + duration + 'ms');
	    
	    callback(null, result);
	});
    });
};
