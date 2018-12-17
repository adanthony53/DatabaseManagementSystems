-- add all your SQL setup statements here. 

-- You can assume that the following base table has been created with data loaded for you when we test your submission 
-- (you still need to create and populate it in your instance however),
-- although you are free to insert extra ALTER COLUMN ... statements to change the column 
-- names / types if you like.

--FLIGHTS (fid int, 
--         month_id int,        -- 1-12
--         day_of_month int,    -- 1-31 
--         day_of_week_id int,  -- 1-7, 1 = Monday, 2 = Tuesday, etc
--         carrier_id varchar(7), 
--         flight_num int,
--         origin_city varchar(34), 
--         origin_state varchar(47), 
--         dest_city varchar(34), 
--         dest_state varchar(46), 
--         departure_delay int, -- in mins
--         taxi_out int,        -- in mins
--         arrival_delay int,   -- in mins
--         canceled int,        -- 1 means canceled
--         actual_time int,     -- in mins
--         distance int,        -- in miles
--         capacity int, 
--         price int            -- in $             
--         )

CREATE TABLE CARRIERS(
    cid varchar(7),
    name varchar(83)
);

CREATE TABLE MONTHS(
    mid int, 
    month varchar(9)
);

CREATE TABLE WEEKDAYS(
    did int,
    day_of_week varchar(9)
);



DROP TABLE IF EXISTS FLIGHTS;
CREATE TABLE FLIGHTS (fid int PRIMARY KEY, 
    month_id int,        -- 1-12
    day_of_month int,    -- 1-31 
    day_of_week_id int,  -- 1-7, 1 = Monday, 2 = Tuesday, etc
    carrier_id varchar(7), 
    flight_num int,
    origin_city varchar(34), 
    origin_state varchar(47), 
    dest_city varchar(34), 
    dest_state varchar(46), 
    departure_delay int, -- in mins
    taxi_out int,        -- in mins
    arrival_delay int,   -- in mins
    canceled int,        -- 1 means canceled
    actual_time int,     -- in mins
    distance int,        -- in miles
    capacity int, 
    price int            -- in $             
);


DROP TABLE IF EXISTS Users;
CREATE TABLE Users (
    username varchar(255) PRIMARY KEY,
    password varchar(255),
    balance int
);

DROP TABLE IF EXISTS Booking;
CREATE TABLE Booking (
    fid INT PRIMARY KEY,
    count INT,
    FOREIGN KEY(fid) REFERENCES Flights(fid)
);

DROP TABLE IF EXISTS Reservation;
CREATE TABLE Reservation (
    username VARCHAR(255) NOT NULL,
    rid INT PRIMARY KEY,
    fid1 INT NOT NULL,
    fid2 INT,
    date INT,
    paid varchar(255) NOT NULL,
    PRIMARY KEY(rid),
    UNIQUE(username, date),
    FOREIGN KEY(username) REFERENCES Users(username),
    FOREIGN KEY(fid1) REFERENCES Booking(fid),
    FOREIGN KEY(fid2) REFERENCES Booking(fid)
);
