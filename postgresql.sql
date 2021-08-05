SET search_path TO A2;

--If you define any views for a question (you are encouraged to), you must drop them
--after you have populated the answer table for that question.
--Good Luck!

--Query 1

CREATE TABLE IF NOT EXISTS query1(
    dname       CHAR(20)
);
INSERT INTO query1 (
    SELECT dname
    FROM department NATURAL JOIN instructor
    WHERE idegree != 'PhD'
    GROUP BY dcode, dname
    HAVING COUNT(iid) = MAX(COUNT(iid))
);

--Query 2
CREATE TABLE IF NOT EXISTS query2 (
    num         INTEGER
);
INSERT INTO query2
(
    SELECT COUNT(sid) AS num
    FROM student NATURAL JOIN department
    WHERE yearofstudy = 4 
        AND dname = 'Computer Science'
        AND sex = 'F'
    GROUP BY sid
);

--Query 3
CREATE TABLE IF NOT EXISTS query3(
    year        INTEGER,
    enrollment  INTEGER
);
INSERT INTO query3
(
    SELECT year, COUNT(sid) AS enrollment
    FROM course 
        NATURAL JOIN courseSection
        NATURAL JOIN department
        NATURAL JOIN studentCourse
    WHERE dname = 'Computer Science'
    GROUP BY cid, year
    HAVING COUNT(sid) = MAX(COUNT(sid))
);

--Query 4
CREATE TABLE IF NOT EXISTS query4 (
    cname       CHAR(20)
);
INSERT INTO query4
(
    SELECT DISTINCT cname
    FROM course 
        NATURAL JOIN courseSection
        NATURAL JOIN department
    WHERE dname = 'Computer Science'
        AND semester = 5
        AND cid NOT IN (
            SELECT cid
            FROM courseSection
            WHERE dcode = 'Computer Science'
                AND (semester = 1 OR semester = 9)
        )
);

--Query 5
CREATE TABLE IF NOT EXISTS query5(
    dept        CHAR(20),
    sid         INTEGER,
    sfirstname  CHAR(20),
    slastname   CHAR(20),
    avgGrade    NUMERIC(10, 5)
);
CREATE VIEW studentGrade AS
(
    SELECT sid, AVG(grade) AS avgGrade
    FROM student NATURAL JOIN studentCourse NATURAL JOIN courseSection
    courseSection.year * 10 + courseSection.semester < (
        SELECT max(cs.year * 10 + cs.semester)
        FROM courseSection cs
    )
    GROUP BY sid
);

CREATE VIEW departmentGrade AS
(
    SELECT dcode, MAX(avgGrade) AS maxGrade
    FROM student
        NATURAL JOIN studentGrade
        NATURAL JOIN studentCourse
    GROUP BY dcode
);

INSERT INTO query5
(
    SELECT dname AS dept, sid, sfirstname, slastname, avgGrade
    FROM student
        NATURAL JOIN studentGrade
        NATURAL JOIN department
        NATURAL JOIN departmentGrade
    WHERE avgGrade = maxGrade
);

DROP VIEW studentGrade;
DROP VIEW departmentGrade;

--Query 6

CREATE TABLE IF NOT EXISTS query6 (
    fname       CHAR(20),
    lname       CHAR(20),
    cname       CHAR(20),
    year        INTEGER,
    semester    INTEGER
);
INSERT INTO query6 (fname, lname, cname, year, semester) (
    SELECT 
        s.sfirstname AS fname,
        s.slastname AS lname,
        c.cname AS cname,
        cs1.year,
        cs1.semester AS semester
    FROM studentCourse sc1, courseSection cs1, student s, course c
    WHERE sc1.csid = cs1.csid AND sc1.sid = s.sid AND cs1.cid = c.cid 
        AND EXISTS (
        -- This subquery SELECT all prerequisite of a course that the student had not taken "earlier on"
        SELECT p.pcid 
        FROM prerequisites p
        WHERE p.cid = cs1.cid AND p.pcid NOT IN (  
            -- This subquery SELECT all cid the student took "earlier on"
            SELECT cs2.cid
            FROM studentCourse sc2, courseSection cs2
            WHERE 
                sc1.sid = sc2.sid AND sc2.csid = cs2.csid  
                AND cs2.year * 10 + cs2.semester < cs1.year * 10 + cs1.semester
        )
    )
);

--Query 7
CREATE TABLE IF NOT EXISTS query7 (
    cname       CHAR(20),
    semester    INTEGER,
    year        INTEGER,
    avgmark     NUMERIC(10, 5)
);

CREATE VIEW threeStudents AS 
(
    SELECT sc.csid, avg(sc.grade) AS avgmark
    FROM studentcourse sc, coursesection cs, department d
    WHERE sc.csid = cs.csid AND cs.dcode = d.dcode AND d.dname = 'Computer Science'
    GROUP BY sc.csid
    HAVING count(*) >= 3
);


INSERT INTO query7 (cname, semester, year, avgmark) (
    SELECT course.cname, 
        cs.semester,
        cs.year,
        ts.avgmark
    FROM threeStudents ts, coursesection cs, course
    WHERE ts.csid = cs.csid AND cs.cid = course.cid AND
        (ts.avgmark = (
            SELECT max(avgmark)
            FROM threeStudents
        ) OR ts.avgmark = (
            SELECT min(avgmark)
            FROM threeStudents
        ))
);

DROP VIEW threeStudents

