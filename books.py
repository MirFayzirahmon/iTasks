import re, json, mysql.connector

with open("/Users/uzmacbook/Downloads/task1_d.json") as f:
    books = json.loads(re.sub(r':(\w+)=>', r'"\1":', f.read()))

conn = mysql.connector.connect(host="localhost", user="root", password="FZ06050402.", database="task1")
cur = conn.cursor()

cur.execute("DROP TABLE IF EXISTS books")
cur.execute("""CREATE TABLE books (
    id VARCHAR(50) PRIMARY KEY, title VARCHAR(500), author VARCHAR(500),
    genre VARCHAR(200), publisher VARCHAR(300), year INT,
    price VARCHAR(20), currency CHAR(1), price_amount DECIMAL(10,2))""")

for i in range(0, len(books), 500):
    values = [(str(b["id"]), b["title"], b["author"], b["genre"], b["publisher"], 
               b["year"], b["price"], b["price"][0], float(b["price"][1:])) 
              for b in books[i:i+500]]
    cur.executemany("INSERT INTO books VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)", values)
conn.commit()

cur.execute("DROP TABLE IF EXISTS summary")
cur.execute("""CREATE TABLE summary AS
    SELECT year AS publication_year, COUNT(*) AS book_count,
           ROUND(AVG(CASE WHEN currency='€' THEN price_amount*1.2 ELSE price_amount END), 2) AS average_price_usd
    FROM books GROUP BY year ORDER BY year""")
conn.commit()

cur.execute("SELECT COUNT(*) FROM books")
print(f"books: {cur.fetchone()[0]} rows")
cur.execute("SELECT COUNT(*) FROM summary")
print(f"summary: {cur.fetchone()[0]} rows")

conn.close()
