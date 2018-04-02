var fs = require('fs'),
    config = require('config-fp'),
    log = require('log')(config.log);

var cleanTmpFolder = function() {
    var files, tmpFolder = config.tmp;

    try {
	files = fs.readdirSync(tmpFolder);
    } catch(e) {
	log.error(e);
	return; 
    }

    if (files.length > 0) {
        for (var i = 0; i < files.length; i++) {
            var filePath = tmpFolder + '/' + files[i];
            if (fs.statSync(filePath).isFile() && files[i] !== '.gitignore') {
		fs.unlinkSync(filePath);
	    }
        }
    }
};

module.exports = {
    cleanTmpFolder: cleanTmpFolder
};
