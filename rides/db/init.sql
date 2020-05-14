
CREATE TABLE rides(
rideid BIGINT(8) NOT NULL AUTO_INCREMENT,
created_by varchar(200) binary NOT NULL,
users varchar(200) binary NOT NULL,
src INT NOT NULL,
dest INT NOT NULL,
ts TIMESTAMP,
PRIMARY KEY (rideid,created_by,users)	   
);
