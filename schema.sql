DROP TABLE IF EXISTS `tracks`;
CREATE TABLE IF NOT EXISTS `tracks` (
  `id` MEDIUMINT NOT NULL AUTO_INCREMENT,
  `codever` char(4) NOT NULL,
  `name` varchar(255) DEFAULT NULL,
  `length` int(5) NOT NULL,
  `import_date` datetime NOT NULL,
  PRIMARY KEY (`id`,`codever`)
) DEFAULT CHARSET=utf8;

DROP TABLE IF EXISTS `codes`;
CREATE TABLE IF NOT EXISTS `codes` (
  `code` int(7) NOT NULL,
  `time` int(7) NOT NULL,
  `track_id` MEDIUMINT NOT NULL,
  PRIMARY KEY (`code`,`time`,`track_id`),
  FOREIGN KEY (`track_id`) REFERENCES `tracks`(`id`) ON DELETE CASCADE
) DEFAULT CHARSET=utf8;
