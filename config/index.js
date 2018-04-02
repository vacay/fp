/* global __dirname, require, module */

var fs = require('fs');
var path = require('path');

var config;
var config_file = '/home/deploy/vacay/shared/apps.json';

if (fs.existsSync(config_file)) {

    config = JSON.parse(fs.readFileSync(config_file));

    var db = config.servers.filter(function(s) {
	return s.roles.indexOf('db') > -1;
    })[0];

    config.mysql.host = db.internal_ip;

} else {

    config = {
	port: 9001,
	tmp: path.join(__dirname, '/../../tmp'),
	debug: true,
	echoprint: 'echoprint-codegen',

	s3: {
	    key: '',
	    secret: '',
	    bucket: 'vacay',
	    folder: 'development'
	},

	log: {
	    level: 'debug',
	    express_format: '[:date] ":method :url HTTP/:http-version" :status :res[content-length] - :response-time ms ":referrer" :remote-addr'
	},

	mysql: {
	    database: 'vacay_development',
	    host: 'localhost',
	    port: 3306,
	    user: 'root',
	    charset  : 'UTF8_GENERAL_CI'
	},

	fp_store: {
	    database: 'fp_development',
	    host: 'localhost',
	    port: 3306,
	    user: 'root',
	    charset  : 'UTF8_GENERAL_CI'
	}

    };
}

if (!config) {
    throw new Error('Application config missing');
}

module.exports = config;
