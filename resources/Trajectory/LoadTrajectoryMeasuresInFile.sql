LOAD DATA LOCAL INFILE ?
    INTO TABLE Measures
    FIELDS TERMINATED BY ','
    LINES TERMINATED BY '\n'
    IGNORE 6 LINES
    (longitude, latitude, zero, altitude, days, measureDateGMT, measureTimeGMT)
    SET setId = ?;
