/* global require, module, process */

var cluster = require('cluster'),
    os = require('os'),
    config = require('config-fp'),
    log = require('log')(config.log),
    routes = require('./routes'),
    utils = require('./modules/utils'),
    fp = require('./workers/fp');

var express = require('express'),
    bodyParser = require('body-parser'),
    compression = require('compression'),
    methodOverride = require('method-override'),
    morgan = require('morgan'),
    http = require('http');


var app = express();

app.disable('x-powered-by');
app.use(morgan(config.log.express_format));
app.use(compression());
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

app.use(methodOverride(function(req) {
    if (req.body && typeof req.body === 'object' && '_method' in req.body) {
	// look in urlencoded POST bodies and delete it
	var method = req.body._method;
	delete req.body._method;
	return method;
    }
}));

app.use(function (req, res, next) {
    res.header('Access-Control-Allow-Origin', '*');
    res.header('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS');
    res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization, Content-Length, X-Requested-With');

    if ('OPTIONS' === req.method || '/health_check' === req.path) {
	res.sendStatus(200);
    } else {
	next();
    }
});

if (config.ssl) {
    app.use(function(req, res, next) {
	if (req.get('X-Forwarded-Proto') !== 'https') {
	    res.redirect('https://' + req.host + req.url);
	} else {
	    next();
	}
    });
}

routes(app);

var startWorker = function() {
    var fpWorker = fp();
    fpWorker.start();
};

var startAPI = function() {
    var port = config.port;
    var env = process.env.NODE_ENV ? ('[' + process.env.NODE_ENV + ']') : '[development]';
    var server = http.Server(app);

    server.listen(port, function () {
	log.info('listening on ' + port + ' in ' + env);
    });
};

var startCluster = function(onWorker, onExit) {
    if (cluster.isMaster) {
	log.info('Initializing ' + os.cpus().length + ' workers in this cluster.');

	utils.cleanTmpFolder();

	startAPI();

	for (var i = 0; i < os.cpus().length; i++) {
	    cluster.fork();
	}

	cluster.on('exit', onExit);

    } else {
	onWorker();
    }
};

var restartApp = function(worker, code, signal) {
    log.error('worker %d died, code (%s), signal(%s). restarting worker...', worker.process.pid, code, signal);
    cluster.fork();
};

if (config.cluster) {
    startCluster(startWorker, restartApp);
} else {
    startAPI();
    startWorker();
}
