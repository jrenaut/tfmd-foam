#!/usr/bin/env python

import csv
import re
import psycopg2
import os

def scrub(row, col_name):
	return row[col_name] if row[col_name]!='' else 0

def loadstuff():
	try:
		conn = psycopg2.connect("dbname={0} user={1} host={2}".format(
			'tfmd', 'postgres', os.environ['HOSTNAME']))
	except:
	     print "I am unable to connect to the database"
	cur = conn.cursor()

	with open('census_data/Census_Tracts__2010.csv') as csvfile:
		reader = csv.DictReader(csvfile)
		for row in reader:
			cur.execute("insert into census_income ( population, hispanic_population, white_population, "+
			"fagi_total_2010, fagi_median_2010,fagi_total_2013, fagi_median_2013, geoid) "+
			"values (%s,%s,%s,%s,%s,%s,%s,%s)",
			(scrub(row, 'P0010001'),scrub(row, 'P0020002'),scrub(row, 'P0020005'),scrub(row, 'FAGI_TOTAL_2010'),
			scrub(row, 'FAGI_MEDIAN_2010'), scrub(row, 'FAGI_TOTAL_2013'), scrub(row, 'FAGI_MEDIAN_2013'), row['GEOID']))
	conn.commit()
	cur.close()
	conn.close()

if __name__ == "__main__":
	loadstuff()
