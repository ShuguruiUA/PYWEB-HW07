import random
from datetime import date, datetime, timedelta

from faker import Faker
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError

from conf.db import session
from conf.models import Teacher, Student, Group, Subject, Grade

fake = Faker('uk-UA')


def academic_year(start: date, end: date) -> list:
    res = []
    current_date = start
    while current_date <= end:
        if current_date.isoweekday() < 6:
            res.append(current_date)
        current_date += timedelta(1)
    return res


def seeder():
    subject_list = [
        "Вища математика",
        "Дискретна математика",
        "Лінійна Алгебра",
        "Програмування",
        "Теорія імовірності",
        "Історія України",
        "Англійська",
        "Креслення",
        "Фізкультура"
    ]

    group_list = [
        "Група - 1",
        "Група - 2",
        "Група - 3"
    ]

    def seed_groups():
        for _ in range(3):
            group = Group(
                name=group_list[_]
            )
            session.add(group)
        session.commit()

    def seed_students():
        group_ids = session.scalars(select(Group.id)).all()
        for _ in range(30):
            student = Student(
                fullname=fake.name(),
                group_id=random.choice(group_ids)
            )
            session.add(student)
        session.commit()

    def seed_teachers():
        for _ in range(5):
            teacher = Teacher(
                fullname=fake.name()
            )
            session.add(teacher)
        session.commit()

    def seed_subjects():
        teacher_ids = session.scalars(select(Teacher.id)).all()
        for subject in subject_list:
            session.add(Subject(name=subject, teacher_id=random.choice(teacher_ids)))
        session.commit()

    def seed_grades():
        start_date = datetime.strptime('2022.09.15', "%Y.%m.%d")
        end_date = datetime.strptime('2023.05.01', "%Y.%m.%d")
        academic_year_range = academic_year(start_date, end_date)
        student_ids = session.scalars(select(Student.id)).all()
        subject_ids = session.scalars(select(Subject.id)).all()

        for d in academic_year_range:
            random_subject_id = random.choice(subject_ids)
            random_student_id = [random.choice(student_ids) for _ in range(3)]

            for student_id in random_student_id:
                grade = Grade(
                    grade=random.randint(45, 98),
                    grade_date=d,
                    student_id=student_id,
                    subject_id=random_subject_id
                )
                session.add(grade)
        session.commit()

    try:
        seed_groups()
        seed_students()
        seed_teachers()
        seed_subjects()
        seed_grades()
    except SQLAlchemyError as e:
        print(e)
        session.rollback()
    finally:
        session.close()


if __name__ == '__main__':
    seeder()
