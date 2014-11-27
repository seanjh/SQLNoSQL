DROP DATABASE IF EXISTS GeoLifeTrajectories;
CREATE DATABASE GeoLifeTrajectories;
USE GeoLifeTrajectories;

CREATE TABLE IF NOT EXISTS Users (
  userId        INT UNSIGNED,
  CONSTRAINT pk_UserId PRIMARY KEY (userId)
);

CREATE TABLE IF NOT EXISTS Sets (
    setId       INT UNSIGNED,
    startTime   DATETIME NOT NULL,
    userId      INT UNSIGNED NOT NULL,
    CONSTRAINT pk_SetId PRIMARY KEY (setId),
    CONSTRAINT fk_UserId FOREIGN KEY (userId)
        REFERENCES Users(userId) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS Measures (
    measureId       INT UNSIGNED AUTO_INCREMENT,
    setId           INT UNSIGNED NOT NULL,
    latitude        DECIMAL(20, 12) NOT NULL,
    longitude       DECIMAL(20, 12) NOT NULL,
    zero            SMALLINT,
    altitude        DECIMAL(20, 12) NOT NULL,
    days            INT UNSIGNED NOT NULL,
    measureDateGMT  DATE,
    measureTimeGMT  TIME,
    CONSTRAINT pk_MeasureId PRIMARY KEY (measureId),
    CONSTRAINT fk_SetId FOREIGN KEY (setId) 
        REFERENCES Sets (setId) ON DELETE CASCADE
);
