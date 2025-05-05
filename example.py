from dbcsv import dbapi2

conn = dbapi2.connect("http://127.0.0.1/schema3", user="johndoe", password="secret")
cur = conn.cursor()

# cur.execute(
#     "SELECT * FROM table2 WHERE (age > 30 AND age < 32) OR name = 'Michael Brown'",
# )
# print(cur.fetchall())

# cur.execute(
#     "SELECT * FROM table2",
# )
# print(cur.fetchmany(4))

# cur.execute(
#     "SELECT * FROM table2 WHERE 'age' = 'age'",
# )
# print(cur.fetchmany(4))

# cur.close()

cur.execute(
    "SELECT * FROM student WHERE birth_date < '2000-01-01' AND (TRUE > student_id OR student_id > 1) or gpa > 3.5"
)
for row in cur.fetchmany(4):
    print(row)

for row in cur.fetchmany(4):
    print(row)

cur.close()
conn.close()
