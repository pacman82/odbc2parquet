
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
    ('abcde', 1.23, 1.23, 42, 42, 1.23, 1.23, 1.23, 'Hello, World!', '2020-09-16', '03:54:12', '2020-09-16 03:54:12');
