from sqlalchemy import func, desc, select, and_, distinct

import argparse

from conf.models import Grade, Teacher, Student, Group, Subject
from conf.db import session

parser = argparse.ArgumentParser(prog='Working with PostgreSQL')
parser.add_argument("-a",
                    "--action",
                    help="Do some action from the list: create, update, list, remove"
                    )
parser.add_argument(
    "create",
    help="Create a new entry in DB"
)
parser.add_argument(
    "list",
    help="print the table"
)
parser.add_argument(
    "-m",
    "--model",
    help="Chosee model: Student, Teacher, Group, "

)
parser.add_argument(
    "-n",
    "--name",
    help="user name"
)
parser.add_argument(
    "--id",
    help="use id"
)
args = vars(parser.parse_args())

