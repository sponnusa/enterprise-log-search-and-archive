CREATE TABLE programs (
	id INT UNSIGNED NOT NULL PRIMARY KEY,
	program VARCHAR(255) NOT NULL,
	pattern VARCHAR(255),
	UNIQUE KEY (program)
) ENGINE=InnoDB;

INSERT INTO programs (id, program) VALUES (1, "none");

CREATE TABLE classes (
	id SMALLINT UNSIGNED NOT NULL PRIMARY KEY,
	class VARCHAR(255) NOT NULL,
	parent_id SMALLINT UNSIGNED NOT NULL DEFAULT 0,
	UNIQUE KEY (class)
) ENGINE=InnoDB;

INSERT INTO classes (id, class, parent_id) VALUES(0, "any", 0);
INSERT INTO classes (id, class, parent_id) VALUES(1, "none", 0);
INSERT INTO classes (id, class, parent_id) VALUES(2, "FIREWALL_ACCESS_DENY", 0);
INSERT INTO classes (id, class, parent_id) VALUES(3, "FIREWALL_CONNECTION_END", 0);
INSERT INTO classes (id, class, parent_id) VALUES(4, "WINDOWS", 0);
INSERT INTO classes (id, class, parent_id) VALUES(7, "URL", 0);
INSERT INTO classes (id, class, parent_id) VALUES(8, "SNORT", 0);
INSERT INTO classes (id, class, parent_id) VALUES(11, "SSH_LOGIN", 0);
INSERT INTO classes (id, class, parent_id) VALUES(12, "SSH_ACCESS_DENY", 0);
INSERT INTO classes (id, class, parent_id) VALUES(13, "SSH_LOGOUT", 0);

CREATE TABLE class_program_map (
	class_id SMALLINT UNSIGNED NOT NULL,
	program_id INT UNSIGNED NOT NULL,
	PRIMARY KEY (class_id, program_id),
	FOREIGN KEY (class_id) REFERENCES classes (id) ON UPDATE CASCADE ON DELETE CASCADE,
	FOREIGN KEY (program_id) REFERENCES programs (id) ON UPDATE CASCADE ON DELETE CASCADE	
) ENGINE=InnoDB;

CREATE TABLE fields (
	id SMALLINT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
	field VARCHAR(255) NOT NULL,
	field_type ENUM("string", "int") NOT NULL,
	pattern_type ENUM("NONE", "QSTRING", "ESTRING", "STRING", "DOUBLE", "NUMBER", "IPv4", "PCRE-IPv4") NOT NULL,
	input_validation VARCHAR(255)
) ENGINE=InnoDB;

INSERT INTO fields (field, field_type, pattern_type) VALUES ("timestamp", "int", "NONE");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("minute", "int", "NONE");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("hour", "int", "NONE");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("day", "int", "NONE");

INSERT INTO fields (field, field_type, pattern_type) VALUES ("host_id", "int", "NONE");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("program_id", "int", "NONE");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("class_id", "int", "NONE");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("msg", "string", "NONE");

INSERT INTO fields (field, field_type, pattern_type, input_validation) VALUES ("ip", "int", "PCRE-IPv4", "IPv4");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("proto", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("o_int", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type, input_validation) VALUES ("srcip", "int", "IPv4", "IPv4");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("srcport", "int", "NUMBER");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("i_int", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type, input_validation) VALUES ("dstip", "int", "IPv4", "IPv4");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("dstport", "int", "NUMBER");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("access_group", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("conn_duration", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("conn_bytes", "int", "NUMBER");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("eventid", "int", "NUMBER");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("source", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("user", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("field0", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("type", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("hostname", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("category", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("site", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("method", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("uri", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("referer", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("user_agent", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("domains", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("status_code", "int", "NUMBER");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("content_length", "int", "NUMBER");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("country_code", "int", "NUMBER");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("sig_sid", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("sig_msg", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("sig_classification", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("sig_priority", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type, input_validation) VALUES ("host", "int", "IPv4", "IPv4");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("authmethod", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("device", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("service", "string", "QSTRING");
INSERT INTO fields (field, field_type, pattern_type) VALUES ("port", "int", "NUMBER");

CREATE TABLE fields_classes_map (
	field_id SMALLINT UNSIGNED NOT NULL,
	class_id SMALLINT UNSIGNED NOT NULL,
	field_order TINYINT UNSIGNED NOT NULL DEFAULT 0,
	PRIMARY KEY (field_id, class_id),
	FOREIGN KEY (field_id) REFERENCES fields (id) ON UPDATE CASCADE ON DELETE CASCADE,
	FOREIGN KEY (class_id) REFERENCES classes (id) ON UPDATE CASCADE ON DELETE CASCADE
) ENGINE=InnoDB;
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES (0, (SELECT id FROM fields WHERE field="host"), 1);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FIREWALL_ACCESS_DENY"), (SELECT id FROM fields WHERE field="proto"), 6);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FIREWALL_ACCESS_DENY"), (SELECT id FROM fields WHERE field="o_int"), 12);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FIREWALL_ACCESS_DENY"), (SELECT id FROM fields WHERE field="srcip"), 7);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FIREWALL_ACCESS_DENY"), (SELECT id FROM fields WHERE field="srcport"), 8);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FIREWALL_ACCESS_DENY"), (SELECT id FROM fields WHERE field="i_int"), 13);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FIREWALL_ACCESS_DENY"), (SELECT id FROM fields WHERE field="dstip"), 9);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FIREWALL_ACCESS_DENY"), (SELECT id FROM fields WHERE field="dstport"), 10);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FIREWALL_ACCESS_DENY"), (SELECT id FROM fields WHERE field="access_group"), 14);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FIREWALL_CONNECTION_END"), (SELECT id FROM fields WHERE field="proto"), 6);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FIREWALL_CONNECTION_END"), (SELECT id FROM fields WHERE field="o_int"), 12);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FIREWALL_CONNECTION_END"), (SELECT id FROM fields WHERE field="srcip"), 7);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FIREWALL_CONNECTION_END"), (SELECT id FROM fields WHERE field="srcport"), 8);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FIREWALL_CONNECTION_END"), (SELECT id FROM fields WHERE field="i_int"), 13);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FIREWALL_CONNECTION_END"), (SELECT id FROM fields WHERE field="dstip"), 9);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FIREWALL_CONNECTION_END"), (SELECT id FROM fields WHERE field="dstport"), 10);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FIREWALL_CONNECTION_END"), (SELECT id FROM fields WHERE field="conn_duration"), 14);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="FIREWALL_CONNECTION_END"), (SELECT id FROM fields WHERE field="conn_bytes"), 11);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="WINDOWS"), (SELECT id FROM fields WHERE field="eventid"), 6);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="WINDOWS"), (SELECT id FROM fields WHERE field="source"), 12);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="WINDOWS"), (SELECT id FROM fields WHERE field="user"), 13);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="WINDOWS"), (SELECT id FROM fields WHERE field="field0"), 14);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="WINDOWS"), (SELECT id FROM fields WHERE field="type"), 15);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="WINDOWS"), (SELECT id FROM fields WHERE field="hostname"), 16);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="WINDOWS"), (SELECT id FROM fields WHERE field="category"), 17);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="URL"), (SELECT id FROM fields WHERE field="srcip"), 6);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="URL"), (SELECT id FROM fields WHERE field="dstip"), 7);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="URL"), (SELECT id FROM fields WHERE field="status_code"), 8);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="URL"), (SELECT id FROM fields WHERE field="content_length"), 9);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="URL"), (SELECT id FROM fields WHERE field="country_code"), 10);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="URL"), (SELECT id FROM fields WHERE field="method"), 12);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="URL"), (SELECT id FROM fields WHERE field="site"), 13);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="URL"), (SELECT id FROM fields WHERE field="uri"), 14);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="URL"), (SELECT id FROM fields WHERE field="referer"), 15);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="URL"), (SELECT id FROM fields WHERE field="user_agent"), 16);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="URL"), (SELECT id FROM fields WHERE field="domains"), 17);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="SNORT"), (SELECT id FROM fields WHERE field="sig_sid"), 12);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="SNORT"), (SELECT id FROM fields WHERE field="sig_msg"), 13);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="SNORT"), (SELECT id FROM fields WHERE field="sig_classification"), 14);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="SNORT"), (SELECT id FROM fields WHERE field="sig_priority"), 6);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="SNORT"), (SELECT id FROM fields WHERE field="proto"), 7);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="SNORT"), (SELECT id FROM fields WHERE field="srcip"), 8);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="SNORT"), (SELECT id FROM fields WHERE field="srcport"), 9);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="SNORT"), (SELECT id FROM fields WHERE field="dstip"), 10);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="SNORT"), (SELECT id FROM fields WHERE field="dstport"), 11);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="SSH_LOGIN"), (SELECT id FROM fields WHERE field="authmethod"), 12);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="SSH_LOGIN"), (SELECT id FROM fields WHERE field="user"), 13);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="SSH_LOGIN"), (SELECT id FROM fields WHERE field="device"), 14);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="SSH_LOGIN"), (SELECT id FROM fields WHERE field="port"), 6);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="SSH_LOGIN"), (SELECT id FROM fields WHERE field="service"), 15);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="SSH_ACCESS_DENY"), (SELECT id FROM fields WHERE field="authmethod"), 12);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="SSH_ACCESS_DENY"), (SELECT id FROM fields WHERE field="user"), 13);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="SSH_ACCESS_DENY"), (SELECT id FROM fields WHERE field="device"), 14);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="SSH_ACCESS_DENY"), (SELECT id FROM fields WHERE field="port"), 6);
INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="SSH_ACCESS_DENY"), (SELECT id FROM fields WHERE field="service"), 15);

INSERT INTO fields_classes_map (class_id, field_id, field_order) VALUES ((SELECT id FROM classes WHERE class="SSH_LOGOUT"), (SELECT id FROM fields WHERE field="user"), 12);


CREATE TABLE table_types (
	id TINYINT UNSIGNED NOT NULL PRIMARY KEY,
	table_type VARCHAR(255) NOT NULL
) ENGINE=InnoDB;
INSERT INTO table_types (id, table_type) VALUES (1, "index");
INSERT INTO table_types (id, table_type) VALUES (2, "archive");

CREATE TABLE tables (
	id SMALLINT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
	table_name VARCHAR(255) NOT NULL,
	start TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	end TIMESTAMP NOT NULL DEFAULT '0000-00-00 00:00:00',
	min_id BIGINT UNSIGNED NOT NULL DEFAULT 1,
	max_id BIGINT UNSIGNED NOT NULL DEFAULT 1,
	table_type_id TINYINT UNSIGNED NOT NULL,
	table_locked_by SMALLINT UNSIGNED,
	FOREIGN KEY (table_type_id) REFERENCES table_types (id),
	UNIQUE KEY (table_name),
	KEY(min_id),
	KEY(max_id)
) ENGINE=InnoDB;

CREATE TABLE indexes (
	id TINYINT UNSIGNED NOT NULL,
	first_id BIGINT UNSIGNED NOT NULL,
	last_id BIGINT UNSIGNED NOT NULL,
	start INT UNSIGNED NOT NULL,
	end INT UNSIGNED NOT NULL,
	table_id SMALLINT UNSIGNED NOT NULL,
	type ENUM("temporary", "permanent", "unavailable") NOT NULL DEFAULT "temporary",
	locked_by SMALLINT UNSIGNED,
	PRIMARY KEY (id, type),
	UNIQUE KEY (first_id, last_id),
	KEY(start),
	KEY(end),
	KEY(type),
	KEY(locked_by),
	FOREIGN KEY (table_id) REFERENCES tables (id) ON UPDATE CASCADE ON DELETE CASCADE
) ENGINE=InnoDB;

CREATE OR REPLACE VIEW v_directory AS
SELECT indexes.id, tables.start, tables.end, min_id, max_id, first_id, last_id, table_name,
UNIX_TIMESTAMP(tables.start) AS table_start_int, UNIX_TIMESTAMP(tables.end) AS table_end_int, 
table_types.table_type, tables.id AS table_id,
type, locked_by, table_locked_by,
FROM_UNIXTIME(indexes.start) AS index_start, FROM_UNIXTIME(indexes.end) AS index_end,
indexes.start AS index_start_int, indexes.end AS index_end_int
FROM tables
JOIN table_types ON (tables.table_type_id=table_types.id)
LEFT JOIN indexes ON (tables.id=indexes.table_id);

CREATE TABLE `syslogs_template` (
  `id` bigint unsigned NOT NULL PRIMARY KEY AUTO_INCREMENT,
  `timestamp` INT UNSIGNED NOT NULL DEFAULT 0,
  `host_id` INT UNSIGNED NOT NULL DEFAULT '1',
  `program_id` INT UNSIGNED NOT NULL DEFAULT '1',
  `class_id` SMALLINT unsigned NOT NULL DEFAULT '1',
  msg TEXT,
  i0 INT UNSIGNED,
  i1 INT UNSIGNED,
  i2 INT UNSIGNED,
  i3 INT UNSIGNED,
  i4 INT UNSIGNED,
  i5 INT UNSIGNED,
  s0 VARCHAR(255),
  s1 VARCHAR(255),
  s2 VARCHAR(255),
  s3 VARCHAR(255),
  s4 VARCHAR(255),
  s5 VARCHAR(255)
) ENGINE=MyISAM;

CREATE TABLE `init` LIKE `syslogs_template`;

CREATE TABLE stats (
	timestamp timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	type ENUM("load", "archive", "index") NOT NULL,
	bytes BIGINT UNSIGNED NOT NULL,
	count BIGINT UNSIGNED NOT NULL,
	time FLOAT UNSIGNED NOT NULL,
	PRIMARY KEY (timestamp, type),
	KEY (type)
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS current_indexes (
	node INT UNSIGNED NOT NULL PRIMARY KEY,
	timestamp timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
	indexes TEXT NOT NULL
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS buffers (
	id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
	filename VARCHAR(255) NOT NULL,
	pid SMALLINT UNSIGNED,
	timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
	index_complete BOOLEAN NOT NULL DEFAULT 0,
	archive_complete BOOLEAN NOT NULL DEFAULT 0,
	UNIQUE KEY (filename)
) ENGINE=InnoDB;

CREATE OR REPLACE VIEW v_indexes AS
SELECT id, type, FROM_UNIXTIME(start) AS start, FROM_UNIXTIME(end) AS end, last_id-first_id AS records, locked_by
FROM indexes;