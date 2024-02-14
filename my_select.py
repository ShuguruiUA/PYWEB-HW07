from sqlalchemy import func, desc, select, and_, distinct

from conf.models import Grade, Teacher, Student, Group, Subject
from conf.db import session

import sys


def select_01():
    """
    SELECT
        s.id,
        s.fullname,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM students s
    JOIN grades g ON s.id = g.student_id
    GROUP BY s.id
    ORDER BY average_grade DESC
    LIMIT 5;
    """
    result = session.query(Student.id, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Student).join(Grade).group_by(Student.id).order_by(desc('average_grade')).limit(5).all()
    return print(result)


def select_02():
    """
    SELECT
        s.id,
        s.fullname,
        ROUND(AVG(g.grade), 2) AS average_grade
    FROM grades g
    JOIN students s ON s.id = g.student_id
    where g.subject_id = 1
    GROUP BY s.id
    ORDER BY average_grade DESC
    LIMIT 1;
    """
    result = session.query(Student.id, Student.fullname, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Grade).join(Student).filter(Grade.subject_id == 1).group_by(Student.id).order_by(
        desc('average_grade')).limit(1).all()
    return print(result)


def select_03():
    """
    SELECT
        groups.name AS group_name,
        subjects.name AS discipline_name,
        ROUND(AVG(grades.grade), 2) AS average_grade
    from grades
    join students ON grades.student_id = students.id
    join groups ON students.group_id = groups.id
    join subjects  ON grades.subject_id = subjects.id
    where subjects.id = 2
    GROUP BY
        groups.name,
        subjects.name
    order by
        groups.name;
    """
    result = session.query(Group.name, Subject.name, func.round(func.avg(Grade.grade), 2).label('average_grade')) \
        .select_from(Grade).join(Student).join(Group).join(Subject).filter(Subject.id == 9) \
        .group_by(Group.name, Subject.name).order_by(Group.name).all()
    return print(result)


def select_04():
    """
    SELECT
        groups.name AS group_name,
        students.fullname AS student_name,
        ROUND(AVG(gr.grade), 2) AS avenger_grade
    FROM grades gr
    JOIN students ON gr.student_id = students.id
    JOIN groups ON students.group_id = groups.id
    JOIN subjects ON gr.subject_id = subjects.id
    GROUP BY
        groups.name,
        students.fullname ;
    """
    result = session.query(Group.name, Student.fullname, func.round(func.avg(Grade.grade))) \
        .select_from(Grade).join(Student).join(Group).group_by(Group.name, Student.fullname).all()
    return print(result)


def select_05():
    """SELECT
        teachers.fullname AS teacher,
        subjects.name AS discipline_name
    from subjects
    JOIN teachers on subjects.teacher_id = teachers.id
    where teacher_id = 2
    GROUP by teachers.fullname, subjects.name;"""

    result = session.query(Teacher.fullname, Subject.name).select_from(Subject).join(Teacher).filter(Teacher.id == 2) \
        .group_by(Teacher.fullname, Subject.name).all()
    return print(result)


def select_06():
    """
````SELECT
        students.fullname AS student_name,
        groups.name AS group_name
    from students
    JOIN groups on groups.id = students.group_id
    where groups.id = 2
    """

    result = session.query(Student.fullname, Group.name).select_from(Student).join(Group) \
        .filter(Group.id == 2).all()
    return print(result)


def select_07():
    """
    select
        students.fullname AS student_name,
        groups.name AS group_name,
        subjects.name AS subjects_name,
        grades.grade AS student_grade
    FROM grades
    JOIN students on grades.student_id = students.id
    JOIN groups on students.group_id = groups.id
    JOIN subjects on grades.subject_id = subjects.id
    where groups.id = 1 AND subjects.id = 9
    ORDER BY students.fullname"""

    result = session.query(Student.fullname, Group.name, Subject.name, Grade.grade).select_from(Grade) \
        .join(Student).join(Group).join(Subject).filter(and_(Group.id == 1, Subject.id == 9)) \
        .order_by(Student.fullname).all()
    return print(result)


def select_08():
    """
    SELECT
        teachers.fullname AS teacher_name,
        subjects.name AS discipline_name,
        ROUND(AVG(grades.grade), 2) AS average_grade
    FROM subjects
    JOIN teachers ON subjects.teacher_id = teachers.id
    JOIN grades ON grades.student_id = subjects.id
    WHERE teachers.id = '3'
    GROUP by
        teachers.fullname,
        subjects.name;
    """
    result = session.query(Teacher.fullname, Subject.name, func.round(func.avg(Grade.grade))).select_from(Subject) \
        .join(Teacher).join(Grade).filter(Teacher.id == 3).group_by(Teacher.fullname, Subject.name).all()
    return print(result)


def select_09():
    """
    SELECT
        students.fullname AS student_name,
        subjects.name AS discipline_name
    FROM subjects 
    JOIN grades ON grades.subject_id  = subjects.id
    JOIN students ON grades.student_id = students.id
    WHERE students.id = '1'
    GROUP BY 
        student_name, 
        discipline_name;
    """
    result = session.query(Student.fullname, Subject.name).select_from(Subject).join(Grade).join(Student) \
        .filter(Student.id == 1).group_by(Student.fullname, Subject.name).all()
    return print(result)


def select_10():
    """
    select distinct
        students.fullname AS student_name,
        subjects.name AS discipline_name,
        teachers.fullname AS teacher_name
    FROM subjects
    JOIN grades ON grades.subject_id  = subjects.id
    JOIN teachers ON subjects.teacher_id = teachers.id
    JOIN students ON grades.student_id = students.id
    WHERE students.id = 30 --AND teacher_id = 1
    GROUP BY
        students.fullname, subjects.name, teachers.fullname ;
    """
    result = session.query(Student.fullname, Subject.name, Teacher.fullname) \
        .select_from(Subject) \
        .join(Grade).join(Teacher).join(Student).filter(Student.id == 30) \
        .group_by(Student.fullname, Subject.name, Teacher.fullname).all()
    return print(result)


def select_11():
    """
    SELECT ROUND(AVG(grades.grade)) as Avenger_grade
    FROM grades
    JOIN students on grades.student_id = students.id
    JOIN subjects on grades.subject_id = subjects.id
    WHERE grades.student_id = 28 and subjects.teacher_id = 1
    """

    result = session.query(func.round(func.avg(Grade.grade)).label('Avenger_grade')).select_from(Grade).join(Subject) \
        .filter(and_(Subject.teacher_id == 5, Grade.student_id == 3)).all()
    return print(result)


def select_12():
    """
    select max(grade_date)
    from grades g
    join students s on s.id = g.student_id
    where g.subject_id = 2 and s.group_id  =3;

    select s.id, s.fullname, g.grade, g.grade_date
    from grades g
    join students s on g.student_id = s.id
    where g.subject_id = 2 and s.group_id = 3 and g.grade_date = (
        select max(grade_date)
        from grades g2
        join students s2 on s2.id=g2.student_id
        where g2.subject_id = 2 and s2.group_id = 3
    );
    :return:
    """

    subquery = (select(func.max(Grade.grade_date)).join(Student).filter(and_(
        Grade.subject_id == 2, Student.group_id == 3
    ))).scalar_subquery()

    result = session.query(Student.id, Student.fullname, Grade.grade, Grade.grade_date) \
        .select_from(Grade) \
        .join(Student) \
        .filter(and_(Grade.subject_id == 2, Student.group_id == 3, Grade.grade_date == subquery)).all()

    return print(result)


def select_x():
    """
    SELECT students.fullname AS student, count(grades.grade) AS total_amount_of_grades
    FROM public.students
    JOIN public.grades ON students.id = grades.student_id
    GROUP BY students.fullname;
    :return:
    """
    result = session.query(Student.fullname, func.count(Grade.grade).label('grades')) \
        .select_from(Student).join(Grade).group_by(Student.id).order_by(desc('grades')).limit(5).all()

    return print(f'{(a,b for a,b in result)}')


def main(cmd: str):
    cmd_dict = {
        '1': select_01,
        '2': select_02,
        '3': select_03,
        '4': select_04,
        '5': select_05,
        '6': select_06,
        '7': select_07,
        '8': select_08,
        '9': select_09,
        '10': select_10,
        '11': select_11,
        '12': select_12,
        'x': select_x,
    }
    def func_runner(cmd):
        return cmd_dict.get(cmd)

    if cmd in cmd_dict.keys():
        res = func_runner(cmd)
        if res:
            res()

if __name__ == '__main__':
    main(sys.argv[1])
    # print(select_01())
    # print(select_02())
    # print(select_03())
    # print(select_04())
    # print(select_05())
    # print(select_06())
    # print(select_07())
    # print(select_08())
    # print(select_09())
    # print(select_10())
    # print(select_11())
    # print(select_12())
    # print(select_x())
