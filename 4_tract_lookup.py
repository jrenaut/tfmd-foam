import requests
import psycopg2
import os, traceback

try:
	conn = psycopg2.connect("dbname={0} user={1} host={2}".format(
		'tfmd', 'postgres', os.environ['HOSTNAME']))
except:
     print "I am unable to connect to the database"
cur = conn.cursor()
census_geographies = "https://geocoding.geo.census.gov/geocoder/geographies/onelineaddress"
census_location = "https://geocoding.geo.census.gov/geocoder/locations/onelineaddress"
payload = {'benchmark':'Public_AR_ACS2016', 'vintage':'Census2010_ACS2016', 'format':'json'}

if False:
	payload['address'] = "1226 H ST NE, WASHINGTON DC,  20002"
	r = requests.get(census_geographies, params=payload)
	print r.json()
	geoid = r.json()['result']['addressMatches'][0]["geographies"]['Census Tracts'][0]['GEOID']
	print "Geoid:{0}".format(geoid,)
else:
	cur.execute("""select * from foam_info;""")
	rows = cur.fetchall()
	for row in rows:
		try:
			payload['address']=row[2]
			r = requests.get(census_geographies, params = payload)
			#print row[2]
			tract= r.json()['result']['addressMatches'][0]["geographies"]["Census Tracts"][0]['BASENAME']
			geoid = r.json()['result']['addressMatches'][0]["geographies"]['Census Tracts'][0]['GEOID']
			cur.execute('update foam_info set census_tract=%s, geoid=%s where foam_info_id=%s;', (tract, geoid, row[0]))
		except:
			print "address: [{0}] not updated".format(row[2],)
conn.commit()
cur.close()
conn.close()
