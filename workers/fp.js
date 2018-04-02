/* global require, module, process, setTimeout, clearTimeout */

var uuid = require('uuid'),
    config = require('config-fp'),
    async = require('async'),
    log = require('log')(config.log),
    s3 = require('s3'),
    request = require('request'),
    codegen = require('../modules/codegen'),
    ingest = require('../modules/ingest'),
    fs = require('fs');

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

	    if (err === 'no vitamins available for ingesting') {
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
		codegen.echoprint,
		this.process,
		this.finish
	    ], this.runAgain.bind(this));
	},
	
	select: function(cb) {

	    log.info('selecting a vitamin to import');

	    var self = this;
	    var timeout = setTimeout(cb, 120000, 'select timeout');
	    var query = db('vitamins')
		    .select()
		    .whereNotNull('processed_at')
		    .whereNull('fingerprint_id')
		    .where('duration', '<', 600)
		    .limit(1)
		    .orderByRaw('Rand()');

	    query.exec(function(err, vitamins) {
		var vitamin = vitamins ? vitamins[0] : null;

		if (!vitamin) {
		    clearTimeout(timeout);
		    cb('no vitamins available for ingesting');
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

	process: function(data, cb) {
	    async.parallel({

		ingest: function(next) {
		    ingest(data, next);
		},

		echonest: function(next) {
		    log.debug(data);

		    request.post({
			url: 'http://developer.echonest.com/api/v4/song/identify',
			form: {
			    api_key: 'YNYFKJ25QRGXMD3XZ',
			    query: JSON.stringify(data)
			},
			json: true
		    }, function(e, r, data) {
			if (e) next(e);
			else next(e, data);
		    });
		}
	    }, cb);
	},

	finish: function(result, cb) {

	    log.info('Finished ingesting ' + this.vitamin.id);

	    var fp = result.ingest;
	    var echonest = result.echonest.response;

	    log.debug(fp);
	    log.debug(echonest);

	    fs.unlink(config.tmp + '/' + this.vitamin.id + '.mp3');

	    var timeout = setTimeout(cb, 120000, 'finish timeout');
	    var query = db('vitamins').update({
		fingerprint_id: fp.track_id,
		echonest_id: echonest.songs.length ? echonest.songs[0].id : null
	    }).where('id', this.vitamin.id);

	    query.exec(function(err, rows) {
		clearTimeout(timeout);
		cb(err, rows);
	    });

	}
    };
};
