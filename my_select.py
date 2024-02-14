from sqlalchemy import func, desc, select, and_

from conf.models import Grade, Teacher, Student, Group, Subject
from conf.db import session


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
    return result


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
    return result


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
    return result
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

    return result


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
    return result


if __name__ == '__main__':
    # print(select_01())
    # print(select_02())
    print(select_03())
    print(select_12())
    # print(select_x())
