## Steps
1. Run your docker or connect to yours PostgreSQL and create a new DB - TEST
2. Init alembic and make changes in its configuration (dev.py)
3. Run seed.py to generate random data in the DB tables
4. Run my_select.py to test SQL queries up to 12 different quires  exist inside this script
## Example
```commandline
    python3 my_select.py x
```
where x - commands from 1 to 12