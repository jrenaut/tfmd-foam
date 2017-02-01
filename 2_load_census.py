#!/usr/bin/env python

import csv
import re
import psycopg2
import os

try:
	conn = psycopg2.connect("dbname={0} user={1} host={2}".format(
		'tfmd', 'postgres', os.environ['HOSTNAME']))
except:
     print "I am unable to connect to the database"
cur = conn.cursor()
with open('census_data/2010_DC_census.csv') as csvfile:
	reader = csv.reader(csvfile)
	for row in reader:
		m = re.search('Census Tract (.+?), District of Columbia, District of Columbia', row[2])
		if m:
			census_tract = m.group(1)
		else:
			census_tract=0
		if "(" in row[3]:
			total_pop = row[3][:row[3].find("(")]
		else:
			total_pop = row[3]
		cur.execute("insert into census_age (id1, geoid, census_tract, total_population,years_lt_5,years_5_9,years_10_14,"+
		"years_15_19,years_20_24,years_25_29,years_30_34,years_35_39,years_40_44,years_45_49,years_50_54,"+
		"years_55_59,years_60_64,years_65_69,years_70_74,years_75_79,years_80_84,years_gte_85,"+
		"total_pop_median_age,years_gte_16,years_gte_18,years_gte_21,years_gte_62,years_gte_65)"+
		"values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);",
		(row[0],row[1],census_tract,total_pop,row[5],row[7],row[9],row[11],row[13],row[15],row[17],row[19],row[21],row[23],row[25],row[27],
		row[29],row[31],row[33],row[35],row[37],row[39],row[41],row[43],row[45],row[47],row[49],row[51]))

		cur.execute("insert into census_males (id1, geoid, census_tract,male_pop,males_lt_5,males_5_9,males_10_14,males_15_19,"+
		"males_20_24,males_25_29,males_30_34,males_35_39,males_40_44,males_45_49,males_50_54,males_55_59,"+
		"males_60_64,males_65_69,males_70_74,males_75_79,males_80_84,males_gte_85,male_median_age,"+
		"males_gte_16,males_gte_18,males_gte_21,males_gte_62,males_get_65) values "+
		"(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);",
		(row[0],row[1],census_tract,row[53],row[55],row[57],row[59],row[61],row[63],row[65],row[67],row[69],row[71],row[73],row[75],
		row[77],row[79],row[81],row[83],row[85],row[87],row[89],row[91],row[93],row[95],row[97],row[99],
		row[101]))

		cur.execute("insert into census_females (id1, geoid, census_tract,female_pop,females_lt_5,females_5_9,females_10_14,"+
		"females_15_19,females_20_24,females_25_29,females_30_34,females_35_39,females_40_44,females_45_49,"+
		"females_50_54,females_55_59,females_60_64,females_65_69,females_70_74,females_75_79,females_80_84,"+
		"females_gte_85,female_median_age,females_gte_16,females_gte_18,females_gte_21,females_gte_62,"+
		"females_get_65) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
		(row[0],row[1],census_tract,row[103],row[105],row[107],row[109],row[111],row[113],row[115],row[117],row[119],row[121],row[123],
		row[125],row[127],row[129],row[131],row[133],row[135],row[137],row[139],row[141],row[143],row[145],
		row[147],row[149],row[151]))
		if "(" in row[225]:
			row_225 = row[225][:row[225].find("(")]
		else:
			row_225 = row[225]
		if "(" in row[153]:
			row_153 = row[153][:row[153].find("(")]
		else:
			row_153 = row[153]
		if "(" in row[213]:
			row_213 = row[213][:row[213].find("(")]
		else:
			row_213 = row[213]
		cur.execute("insert into census_race (id1, geoid, census_tract,total_pop_race,total_pop_one_race,"+
		"race_white,race_african_american,race_american_indian,race_asian,race_asian_indian,race_asian_chinese,"+
		"race_asian_filipino,race_asian_japanese,race_asian_korean,race_asian_vietnamese,race_asian_other,"+
		"race_pacific_island,race_native_hawaiian,race_guamanian,race_samoan,race_other_pac_isl,race_other,"+
		"total_pop_multi_race,multi_race_white,multi_race_asian,multi_race_african_american,multi_race_other,"+
		"total_pop_hispanic_latino,race_hispanic_latino,race_mexican,race_puerto_rican,race_cuban,"+
		"race_other_hisp_latino,race_hisp_l_not_hisp_l) values "+
		"(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);",
		(row[0],row[1],census_tract,row_153,row[155],row[157],row[159],row[161],row[163],row[165],row[167],
		row[169],row[171],row[173],row[175],row[177],row[179],row[181],row[183],row[185],row[187],row[189],
		row[191],row[193],row[195],row[197],row[199],row_213,row[215],row[217],row[219],row[221],row[223],
		row_225))
		if "(" in row[261]:
			row_261 = row[261][:row[261].find("(")]
		else:
			row_261 = row[261]
		if "(" in row[263]:
			row_263 = row[263][:row[263].find("(")]
		else:
			row_263 = row[263]
		cur.execute("insert into census_relationship (id1, geoid, census_tract,rel_total_pop,rel_pop_in_households,"+
		"rel_householder,rel_spouse,rel_child,rel_own_child_lt_18,rel_other_rel,rel_other_rel_lt_18,"+
		"rel_other_rel_gte_65,rel_non_rel,rel_non_rel_lt_18,rel_non_rel_gte_65,rel_non_rel_unmarried_partner,"+
		"rel_group,rel_group_institutionalized,rel_group_inst_male,rel_group_inst_female,rel_group_non_inst,"+
		"rel_group_non_inst_male,rel_group_non_inst_female) values"+
		"(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);",
		(row[0],row[1],census_tract,row_261,row_263,row[265],row[267],row[269],row[271],row[273],row[275],row[277],row[279],row[281],
		row[283],row[285],row[287],row[289],row[291],row[293],row[295],row[297],row[299]))

		if "(" in row[301]:
			row_301 = row[301][:row[301].find("(")]
		else:
			row_301 = row[301]
		if "(" in row[339]:
			row_339 = row[339][:row[339].find("(")]
		else:
			row_339 = row[339]
		if "(" in row[341]:
			row_341 = row[341][:row[341].find("(")]
		else:
			row_341 = row[341]
		if "(" in row[343]:
			row_343 = row[343][:row[343].find("(")]
		else:
			row_343 = row[343]
		if "(" in row[353]:
			row_353 = row[353][:row[353].find("(")]
		else:
			row_353 = row[353]
		if "(" in row[355]:
			row_355 = row[355][:row[355].find("(")]
		else:
			row_355 = row[355]
		if "(" in row[357]:
			row_357 = row[357][:row[357].find("(")]
		else:
			row_357 = row[357]
		if "(" in row[361]:
			row_361 = row[361][:row[361].find("(")]
		else:
			row_361 = row[361]
		cur.execute("insert into census_household (id1, geoid, census_tract,total_households,family_households,"+
		"family_own_children_lt_18,family_husb_wife,family_husb_wife_own_children,family_male_only,"+
		"family_male_only_children_lt_18,family_female_only,family_female_only_children_lt_18,non_family,"+
		"householder_only,householder_only_male,hh_only_male_gte_65,householder_only_female,"+
		"hh_only_female_gte_65,hh_lt_18,hh_gte_65,average_household_size,average_family_size,"+
		"total_housing_units,tot_occupied_housing_units,vacant_housing_units,vacant_housing_for_rent,"+
		"vacant_rented_unoccupied,vacant_housing_for_sale,vacant_sold_unoccupied,vacant_seasonal,"+
		"vacant_other,vacancy_rate,rental_vacancy_rate,occupied_housing_units,owner_occupied_housing,"+
		"pop_in_owner_occ_units,avg_size_owner_occ_units,renter_occupied_housing,pop_in_renter_occ_units,"+
		"avg_size_renter_occ_units) values"+
		"(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"+
		"%s,%s,%s,%s,%s,%s,%s,%s);",
		(row[0],row[1],census_tract,row_301,row[303],
		row[305],row[307],row[309],row[311],row[313],row[315],row[317],row[319],row[321],row[323],row[325],
		row[327],row[329],row[331],row[333],row[335],row[337],row_339,row_341,row_343,row[345],row[347],
		row[349],row[351],row_353,row_355,row_357,row[359],row_361,row[363],row[365],row[367],row[369],
		row[371],row[373]))

conn.commit()
cur.close()
conn.close()
