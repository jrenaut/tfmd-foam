#!/usr/bin/env python

import csv
import os
from datetime import datetime
import psycopg2

try:
	conn = psycopg2.connect("dbname={0} user={1} host={2}".format(
		'tfmd', 'postgres', os.environ['HOSTNAME']))
except:
     print "I am unable to connect to the database"
cur = conn.cursor()
with open('2015_foam_info_by_corridor.csv') as csvfile:
	reader = csv.DictReader(csvfile)
	for row in reader:
		print "insert row: {0}".format(row['Business - Site Address'],)
		cur.execute("INSERT INTO foam_info (survey_date, address, foam, corridor) values (%s,%s,%s,%s);",
			(row['Date'], row['Business - Site Address'], row['Foam currently?']=="Yes", row['Corridor - Name'])
		)
conn.commit()
cur.close()
conn.close()
