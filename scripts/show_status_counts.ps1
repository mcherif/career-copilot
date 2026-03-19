@'
import sqlite3

conn = sqlite3.connect("career_copilot.db")
cur = conn.cursor()

print("-- final_status_counts --")
for row in cur.execute("SELECT status, COUNT(*) FROM jobs GROUP BY status ORDER BY COUNT(*) DESC"):
    print(row)

print()
print("-- rule_status_counts --")
for row in cur.execute("SELECT COALESCE(rule_status, 'NULL'), COUNT(*) FROM jobs GROUP BY rule_status ORDER BY COUNT(*) DESC"):
    print(row)

print()
print("-- llm_status_counts --")
for row in cur.execute("SELECT COALESCE(llm_status, 'NULL'), COUNT(*) FROM jobs GROUP BY llm_status ORDER BY COUNT(*) DESC"):
    print(row)

conn.close()
'@ | python -
