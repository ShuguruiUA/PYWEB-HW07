from datetime import datetime, timedelta
import logging

from sqlalchemy import func, desc, select, and_, distinct

import argparse
import random

from conf.models import Grade, Teacher, Student, Group, Subject
from conf.db import session

logging.basicConfig(level=logging.INFO)


def random_date(start_date, end_date, result=None):
    # Convert start_date and end_date to datetime objects
    start_datetime = datetime.strptime(start_date, "%Y.%m.%d")
    end_datetime = datetime.strptime(end_date, "%Y.%m.%d")

    # Calculate the difference in days between start_date and end_date
    delta_days = (end_datetime - start_datetime).days

    # Generate a random number of days to add to start_date
    random_days = random.randint(0, delta_days)
    fix_days = random.randint(1, 4)

    # Add the random number of days to start_date
    random_date = start_datetime + timedelta(days=random_days)

    if random_date.isoweekday() < 6:
        result = random_date
    else:
        result = random_date + timedelta(days=fix_days)
    return result.strftime("%Y.%m.%d")


def create_record(model, name):
    if model == "Grade":
        student_ids = session.scalars(select(Student.id)).all()
        subject_ids = session.scalars(select(Subject.id)).all()
        model_name = eval(model)(
            grade=name,
            student_id=random.choice(student_ids),
            subject_id=random.choice(subject_ids),
            grade_date=random_date('2022.09.15', '2023.05.01')
        )
        session.add(model_name)
        session.commit()
        subqery = session.query(func.max(Grade.id)).scalar()
        query_res = session.query(Grade.grade, Grade.grade_date, Student.fullname, Subject.name) \
            .select_from(Grade).join(Student).join(Subject).filter(Grade.id == subqery).first()

        a, b, c, d = query_res
        logging.info(f"Grade: {a} | Grade date: {b} | Student: {c} | Subject: {d}")
        return print(f'New grade successfully added')

    elif model == "Teacher":
        model_name = eval(model)(
            fullname=name
        )
        session.add(model_name)
        session.commit()
        return print(f'New {model} successfully added to table {model.lower()}s with name "{name}"')
    elif model == "Student":
        group_ids = session.scalars(select(Group.id)).all()
        model_name = eval(model)(
            fullname=name,
            group_id=random.choice(group_ids)
        )
        session.add(model_name)
        session.commit()
        logging.info(f'New {model} successfully added to table {model.lower()}s with name "{name}"')
        return print(f'New {model} successfully added to table {model.lower()}s with name "{name}"')
    elif model == "Group":
        model_name = eval(model)(
            name=name
        )
        session.add(model_name)
        session.commit()
        return print(f'New {model} successfully added to table {model.lower()}s with name "{name}"')

    elif model == "Subject":
        teacher_ids = session.scalars(select(Teacher.id)).all()
        model_name = eval(model)(
            name=name,
            teacher_id=random.choice(teacher_ids)
        )
        session.add(model_name)
        session.commit()
        return print(f'New {model} successfully added to table {model.lower()}s with name "{name}"')


def list_records(model):
    result = session.query(eval(model)).all()
    if model == "Grade":
        for r in result:
            print(f"ID: {r.id} | Grade: {r.grade} | Grade date: {r.grade_date}")
    elif model in ["Student", "Teacher"]:
        for r in result:
            print(f"ID: {r.id} | {model} name: {r.fullname}")
    else:
        for r in result:
            print(f"ID: {r.id} | {model} name: {r.name}")


def update_record(model, ids, name):
    if model in ["Student", "Teacher"]:
        model_update = session.query(eval(model)).get(int(ids))
        model_update.fullname = name
        session.add(model_update)
        session.commit()
    elif model == "Grade":
        model_update = session.query(eval(model)).get(ids)
        model_update.grade = name
        session.add(model_update)
        session.commit()
    else:
        model_update = session.query(eval(model)).get(ids)
        model_update.name = name
        session.add(model_update)
        session.commit()


def remove_record(model, ids):
    model_remove = session.query(eval(model)).get(ids)
    session.delete(model_remove)
    session.commit()


def main():
    parser = argparse.ArgumentParser(description="CLI програма для CRUD операцій із базою даних")
    parser.add_argument("-a", "--action", choices=["create", "list", "update", "remove"],
                        help="Дія для виконання: create, list, update, remove", required=True)
    parser.add_argument("-m", "--model", choices=["Teacher", "Group", "Grade", "Student", "Subject"],
                        help="Модель, над якою виконується операція", required=True)
    parser.add_argument("-id", "--record_id", type=int, help="Ідентифікатор запису для оновлення або видалення")
    parser.add_argument("-n", "--name", help="Значення для створення імені, оцінки або оновлення запису")

    args = parser.parse_args()

    if args.action == "create":
        if args.name:
            create_record(args.model, name=args.name)
        else:
            print("Параметр --name є обов'язковим для дії 'create'")
    elif args.action == "list":
        list_records(args.model)
    elif args.action == "update":
        if args.record_id and args.name:
            update_record(args.model, ids=args.record_id, name=args.name)
        else:
            print("Параметри --id та --name є обов'язковими для дії 'update'")
    elif args.action == "remove":
        if args.record_id:
            remove_record(args.model, args.record_id)
        else:
            print("Параметр --id є обов'язковим для дії 'remove'")


if __name__ == "__main__":
    main()
