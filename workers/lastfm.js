/* global require, module, process, setTimeout, clearTimeout */

var config = require('config-fp'),
    log = require('log')(config.log),
    async = require('async'),
    s3 = require('s3'),
    fs = require('fs'),
    uuid = require('uuid'),
    codegen = require('../modules/codegen');

var s3client = s3.createClient({
    s3Options: {
	accessKeyId: config.s3.key,
	secretAccessKey: config.s3.secret
    }
});

var db = require('knex')({
    client: 'mysql',
    connection: config.mysql
});

module.exports = function() {
    return {
	worker: uuid.v4() + ':' + process.pid,

	vitamin: null,

	runAgain: function(err, rows) {

	    this.vitamin = null;

	    if (err === 'no vitamins missing lastfm_fingerprint_id') {
		log.info(err);
		setTimeout(this.start.bind(this), 10000);
	    } else if (err) {
		log.error(err);
		setTimeout(this.start.bind(this), 10000);
	    } else {
		this.start();
	    }
	},

	start: function() {

	    log.debug('Vitamin Worker ' + this.worker + ' running');

	    async.waterfall([
		this.select,
		this.download,
		codegen.lastfm,
		this.finish
	    ], this.runAgain.bind(this));
	},
	
	select: function(cb) {

	    log.info('selecting a vitamin missing a lastfm_fingerprint_id');

	    var self = this;
	    var timeout = setTimeout(cb, 120000, 'select timeout');
	    var query = db('vitamins')
		    .select()
		    .whereNotNull('processed_at')
		    .whereNull('lastfm_fingerprint_id')
		    .where('duration', '<', 600)
		    .limit(1)
		    .orderByRaw('Rand()');

	    query.exec(function(err, vitamins) {
		var vitamin = vitamins ? vitamins[0] : null;

		if (!vitamin) {
		    clearTimeout(timeout);
		    cb('no vitamins missing lastfm_fingerprint_id');
		} else {
		    self.vitamin = vitamin;
		    clearTimeout(timeout);
		    cb(err, vitamin);
		}
	    });

	},

	download: function(vitamin, cb) {

	    log.info('starting download of vitamin ' + vitamin.id);

	    var timeout = setTimeout(cb, 120000, 'download timeout');
	    var file = config.tmp + '/' + vitamin.id + '.mp3';
	    var params = {
		localFile: file,
		s3Params: {
		    Bucket: config.s3.bucket,
		    Key: config.s3.folder + '/vitamins/' + vitamin.id + '.mp3'
		}
	    };

	    var dl = s3client.downloadFile(params);

	    dl.on('error', function(err) {
		clearTimeout(timeout);
		cb(err, null);
	    });

	    dl.on('end', function() {
		clearTimeout(timeout);
		cb(null, file);
	    });

	},

	finish: function(result, cb) {

	    log.info('Finished ingesting ' + this.vitamin.id);

	    fs.unlink(config.tmp + '/' + this.vitamin.id + '.mp3');

	    console.log(result);
	    cb(null);

	    // var timeout = setTimeout(cb, 120000, 'finish timeout');
	    // var query = db('vitamins').update({
	    // 	lastfm_fingerprint_id: result.track_id
	    // }).where('id', this.vitamin.id);

	    // query.exec(function(err, rows) {
	    // 	clearTimeout(timeout);
	    // 	cb(err, rows);
	    // });

	}
    };
};
