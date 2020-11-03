CREATE TABLE Movies
(
    title VARCHAR(255) NOT NULL,
    year INT
);

INSERT INTO Movies
    (title, year)
Values
    ('Jurassic Park', 1993),
    ('2001: A Space Odyssey', 1968),
    ('Interstellar', NULL);

CREATE TABLE Birthdays
(
    name VARCHAR(255) NOT NULL,
    birthday DATE NOT NULL,
)

INSERT INTO BIRTHDAYS
    (name, birthday)
Values
    ('Keanu Reeves', '1964-09-02'),
    ('Robin Wiliams', '1951-07-21');

CREATE TABLE Sales
(
    id INT PRIMARY KEY,
    day DATE,
    time TIME,
    product INT,
    price DECIMAL(10,2)
)

INSERT INTO SALES
    (id, day, time, product, price)
Values
    (1, '2020-09-09', '00:05:34', 54, 9.99),
    (2, '2020-09-10', '12:05:32', 54, 9.99),
    (3, '2020-09-10', '14:05:32', 34, 2.00);

CREATE TABLE IntegerDecimals
(
    three DECIMAL(3,0),
    nine DECIMAL(9,0),
    eighteen DECIMAL(18,0),
)

INSERT INTO IntegerDecimals
    (three, nine, eighteen)
Values
    (123, 123456789, 123456789012345678);

CREATE Table AllTheTypes
(
    my_char CHAR(5) NOT NULL,
    my_numeric NUMERIC(3,2) NOT NULL,
    my_decimal DECIMAL(3,2) NOT NULL,
    my_integer INTEGER NOT NULL,
    my_smallint SMALLINT NOT NULL,
    my_float FLOAT(3) NOT NULL,
    my_real REAL NOT NULL,
    my_double DOUBLE PRECISION NOT NULL,
    my_varchar VARCHAR(100) NOT NULL,
    my_date DATE NOT NULL,
    my_time Time NOT NULL,
    my_timestamp DATETIME NOT NULL,
)

INSERT INTO AllTheTypes
    (my_char, my_numeric, my_decimal, my_integer, my_smallint, my_float, my_real, my_double, my_varchar, my_date, my_time, my_timestamp)
Values
    ("abcde", 1.23, 1.23, 42, 42, 1.23, 1.23, 1.23, "Hello, World!", '2020-09-16', '03:54:12', '2020-09-16 03:54:12');
