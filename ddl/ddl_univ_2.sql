CREATE TABLE categories (
_id TEXT,
name TEXT,
PRIMARY KEY (_id),
UNIQUE (_id)
);

CREATE TABLE students (
_id TEXT,
name TEXT,
address TEXT,
PRIMARY KEY (_id),
UNIQUE (_id)
);

CREATE TABLE courses (
_id TEXT,
label TEXT,
nbr_hours INT,
level TEXT,
categories TEXT,
PRIMARY KEY (_id),
UNIQUE (_id),
FOREIGN KEY (categories) REFERENCES categories (_id)
);

CREATE TABLE courses_students (
courses_id TEXT,
students_id TEXT,
FOREIGN KEY (courses_id) REFERENCES courses (_id),
FOREIGN KEY (students_id) REFERENCES students (_id)
);