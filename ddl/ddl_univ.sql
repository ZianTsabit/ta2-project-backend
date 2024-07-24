CREATE TABLE students (
_id TEXT,
name TEXT,
address TEXT,
PRIMARY KEY (_id),
UNIQUE (_id)
);

CREATE TABLE courses (
_id TEXT,
student_id TEXT,
label TEXT,
nbr_hours INT,
level TEXT,
PRIMARY KEY (_id),
UNIQUE (_id),
FOREIGN KEY (student_id) REFERENCES students (_id)
);