/* global module */

module.exports = function (app) {

    app.get('*', function (req, res) {
	res.status(404).send({
	    message: 'invalid endpoint: ' + req.url
	});
    });

};
