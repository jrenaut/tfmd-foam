import csv
import psycopg2
import os

try:
	conn = psycopg2.connect("dbname={0} user={1} host={2}".format(
		'tfmd', 'postgres', os.environ['HOSTNAME']))
except:
     print "I am unable to connect to the database"
cur = conn.cursor()

f = open('foam_analysis.csv', 'wt')
try:
	cur.execute("""select * from foam_analysis;""")
	rows = cur.fetchall()
	writer = csv.writer(f)
	writer.writerow( ('id', 'date', 'address', 'foam', 'corridor', 'census tract', 'geoid', 'FAGI 2010', 'FAGI median 2010', 'FAGI 2013', 'FAGI median 2013', 'Majority White', 'Majority Hispanic') )
	for i in rows:
		writer.writerow(i)
finally:
    f.close()
