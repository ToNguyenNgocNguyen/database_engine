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

cur.execute("SELECT * FROM peoplesmall WHERE city = 'Chicago' AND age < 30 OR age > 20")
print(cur.fetchall())
print(cur.rowcount)

cur.close()
conn.close()
