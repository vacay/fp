/* global module, require */

var config = require('config-fp'),
    uuid = require('uuid'),
    fs = require('fs'),
    spawn = require('child_process').spawn,
    log = require('log')(config.log),
    fpcalc = require('fpcalc');

var echoprint = function (path, callback) {
    var stdoutFilename = config.tmp + '/' + uuid.v1() + '.log';
    var stdoutFile = fs.createWriteStream(stdoutFilename);
    var echoprint = spawn(config.echoprint, [path]);

    echoprint.on('error', function (err) {
	callback('[echoprint codegen] ' + err, null);
    });

    echoprint.stdout.pipe(stdoutFile);

    echoprint.on('exit', function () {
	fs.readFile(stdoutFilename, 'utf8', function (err, data) {
	    if (err) {
		callback('[read echoprint codegen output] ' + err, null);
	    } else {
		fs.unlink(stdoutFilename);
		var fp;

		try {
		    fp = JSON.parse(data);
		} catch(e) {
		    log.error(e, { data: data });
		    callback(e, null);
		    return;
		}

		callback(null, fp[0]);
	    }
	});
    });
};

var lastfm = function (path, callback) {
    var stdoutFilename = config.tmp + '/' + uuid.v1() + '.log';
    var stdoutFile = fs.createWriteStream(stdoutFilename);
    var lastfm = spawn(config.lastfm, ['-nometadata', path]);

    lastfm.on('error', function (err) {
	callback('[lastfm codegen] ' + err, null);
    });

    lastfm.stdout.pipe(stdoutFile);

    lastfm.on('exit', function () {
	fs.readFile(stdoutFilename, 'utf8', function (err, data) {
	    if (err) {
		callback('[read lastfm codegen output] ' + err, null);
	    } else {
		fs.unlink(stdoutFilename);
		callback(null, data);
	    }
	});
    });
};

module.exports = {
    echoprint: echoprint,
    lastfm: lastfm,
    chromaprint: fpcalc
};
