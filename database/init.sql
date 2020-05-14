CREATE TABLE users(
    username varchar(200) binary NOT NULL,
    password varchar(40) NOT NULL,
    PRIMARY KEY (username)
);


CREATE TABLE rides(
    rideid BIGINT(8) NOT NULL AUTO_INCREMENT,
    created_by varchar(200) binary NOT NULL,
    users varchar(200) binary NOT NULL,
    src INT NOT NULL,
    dest INT NOT NULL,
    ts TIMESTAMP,
    PRIMARY KEY (rideid,created_by,users),
    FOREIGN KEY (created_by) REFERENCES users(username)
    ON DELETE CASCADE,
    FOREIGN KEY (users) REFERENCES users(username)
    ON DELETE CASCADE	   
);
