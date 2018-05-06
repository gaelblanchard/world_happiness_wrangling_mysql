#required libraries 
import csv
import sqlite3
from pandas import DataFrame
import numpy as np

#TO DO:
# finish data cleanup
#find all important statistical measures

#populate the database
#replace col_name words to reflect the data being inserted
def populate_database(db_name,file_name):
	#Connecting to the sqlite database
	conn = sqlite3.connect(db_name)
	conn.text_factory = str
	c = conn.cursor()
	#creates the table only when it doesnt exist for entry of data from the csv
	#the unique keyword does  a bit of preliminary data cleanup by ensuring
	#that inserted data for country is always unique
	#the not null keyword prevent the entry in ech column is never null
	#should only be used when null values provide no value to the data
	c.execute("CREATE TABLE IF NOT EXISTS world_happiness (Country VARCHAR(255) NOT NULL, Region VARCHAR(255) NOT NULL, HappinessRank UNSIGNED INT NOT NULL, HappinessScore DECIMAL(15,7) NOT NULL, StandardError DECIMAL(15,7) NOT NULL, Economy DECIMAL(15,7) NOT NULL, Family DECIMAL(15,7) NOT NULL, Health DECIMAL(15,7) NOT NULL, Freedom DECIMAL(15,7) NOT NULL, Trust DECIMAL(15,7) NOT NULL, Generosity DECIMAL(15,7) NOT NULL, DystopiaResidual DECIMAL(15,7) NOT NULL, UNIQUE(Country));") #Create table
	with open(file_name,'rt') as fin:
		dr = csv.DictReader(fin)
		#set i["row_name"] for every column in the csv file
		# n must equal insert or ignore into X (i,..n) == VALUES (?0,...?M)   
		to_db = [(i['Country'],i['Region'],i['Happiness Rank'],i['Happiness Score'],i['Standard Error'],i['Economy (GDP per Capita)'],i['Family'],i['Health (Life Expectancy)'],i['Freedom'],i['Trust (Government Corruption)'],i['Generosity'],i['Dystopia Residual']) for i in dr]

	c.executemany("INSERT OR IGNORE INTO world_happiness (Country, Region,HappinessRank,HappinessScore,StandardError,Economy,Family,Health,Freedom,Trust,Generosity,DystopiaResidual) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?);", to_db)
	conn.commit()
	conn.close()

#code to provide cleanup for database using SQL methods
#because we are introducing intentionally false data
#we can delete outliers however in some cases deleting outliers is not
#optimal
def clean_database(db_name):
	conn = sqlite3.connect(db_name)
	conn.text_factory = str
	c = conn.cursor()
	#cleanup for removal of null values. In our case this data doesnt exist but the following query would remove null values
	c.execute("DELETE FROM world_happiness WHERE Country IS NULL OR Region IS NULL OR HappinessRank IS NULL OR HappinessScore IS NULL OR StandardError IS NULL OR Economy IS NULL OR Family IS NULL OR Health IS NULL OR Freedom IS NULL OR Trust IS NULL OR Generosity IS NULL OR DystopiaResidual IS NULL;")
	#Converts the world_happiness table to a dataframe which we can use to calcuate quartiles and determine outliers which we'll then r 
	c.execute("SELECT * FROM world_happiness")
	wh_df = DataFrame(c.fetchall())
	wh_df.columns = ['Country','Region','HappinessRank','HappinessScore','StandardError','Economy','Family','Health','Freedom','Trust','Generosity','DystopiaResidual']
	#wh_df.column_name.quantile gets quartiles
	#determines first and third quartiles and determines inter-quartile range
	#Using this calcuation we use 
	q3, q1 = np.percentile(wh_df.HappinessScore, [75,25])
	iqr = q3 - q1
	outer_constant = 3 * iqr
	lowerbound_outer = q1 - outer_constant
	print(lowerbound_outer)
	upperbound_outer = q3 + outer_constant
	print(upperbound_outer)
	fences = (
		float(upperbound_outer),
		float(lowerbound_outer),
	)
	sql_query = "DELETE FROM world_happiness WHERE HappinessScore > (?);"
	c.executemany(sql_query,(float(upperbound_outer),))
	q3, q1 = np.percentile(wh_df.StandardError, [75,25])
	iqr = q3 - q1
	outer_constant = 3 * iqr
	lowerbound_outer = q1 - outer_constant
	upperbound_outer = q3 + outer_constant
	c.executemany("DELETE FROM world_happiness WHERE StandardError > (?) OR StandardError < (?);",(int(upperbound_outer),int(lowerbound_outer)))
	q3, q1 = np.percentile(wh_df.Economy, [75,25])
	iqr = q3 - q1
	outer_constant = 3 * iqr
	lowerbound_outer = q1 - outer_constant
	upperbound_outer = q3 + outer_constant
	c.executemany("DELETE FROM world_happiness WHERE Economy > (?) OR Economy < (?);",(int(upperbound_outer),int(lowerbound_outer)))
	q3, q1 = np.percentile(wh_df.Family, [75,25])
	iqr = q3 - q1
	outer_constant = 3 * iqr
	lowerbound_outer = q1 - outer_constant
	upperbound_outer = q3 + outer_constant
	c.executemany("DELETE FROM world_happiness WHERE Family > (?) OR Family < (?);",(int(upperbound_outer),int(lowerbound_outer)))
	q3, q1 = np.percentile(wh_df.Health, [75,25])
	iqr = q3 - q1
	outer_constant = 3 * iqr
	lowerbound_outer = q1 - outer_constant
	upperbound_outer = q3 + outer_constant
	c.executemany("DELETE FROM world_happiness WHERE Health > (?) OR Health < (?);",(int(upperbound_outer),int(lowerbound_outer)))
	q3, q1 = np.percentile(wh_df.Freedom, [75,25])
	iqr = q3 - q1
	outer_constant = 3 * iqr
	lowerbound_outer = q1 - outer_constant
	upperbound_outer = q3 + outer_constant
	c.executemany("DELETE FROM world_happiness WHERE Freedom > (?) OR Freedom < (?);",(int(upperbound_outer),int(lowerbound_outer)))
	q3, q1 = np.percentile(wh_df.Trust, [75,25])
	iqr = q3 - q1
	outer_constant = 3 * iqr
	lowerbound_outer = q1 - outer_constant
	upperbound_outer = q3 + outer_constant
	c.executemany("DELETE FROM world_happiness WHERE Trust > (?) OR Trust < (?);",(int(upperbound_outer),int(lowerbound_outer)))
	q3, q1 = np.percentile(wh_df.Generosity, [75,25])
	iqr = q3 - q1
	outer_constant = 3 * iqr
	lowerbound_outer = q1 - outer_constant
	upperbound_outer = q3 + outer_constant
	c.executemany("DELETE FROM world_happiness WHERE Generosity > (?) OR Generosity < (?);",(int(upperbound_outer),int(lowerbound_outer)))
	q3, q1 = np.percentile(wh_df.DystopiaResidual, [75,25])
	iqr = q3 - q1
	outer_constant = 3 * iqr
	lowerbound_outer = q1 - outer_constant
	upperbound_outer = q3 + outer_constant
	c.executemany("DELETE FROM world_happiness WHERE DystopiaResidual > (?) OR DystopiaResidual < (?);",(int(upperbound_outer),int(lowerbound_outer)))
	conn.commit()
	conn.close()

def group_happiness_by_region(db_name):
	#This method will use the GROUP BY SQL method to average happiness score, and other columns by region
	#Will return a row for every region
	conn = sqlite3.connect(db_name)
	conn.text_factory = str
	c = conn.cursor()
	c.execute("CREATE TABLE IF NOT EXISTS region_happiness (Region VARCHAR(255) NOT NULL, HappinessScore DECIMAL(15,7) NOT NULL, StandardError DECIMAL(15,7) NOT NULL, Economy DECIMAL(15,7) NOT NULL, Family DECIMAL(15,7) NOT NULL, Health DECIMAL(15,7) NOT NULL, Freedom DECIMAL(15,7) NOT NULL, Trust DECIMAL(15,7) NOT NULL, Generosity DECIMAL(15,7) NOT NULL, DystopiaResidual DECIMAL(15,7) NOT NULL );")
	c.execute("INSERT OR IGNORE INTO region_happiness (Region,HappinessScore,StandardError,Economy,Family,Health,Freedom,Trust,Generosity,DystopiaResidual) SELECT Region, avg(HappinessScore),avg(StandardError),avg(Economy),avg(Family),avg(Health),avg(Freedom),avg(Trust),avg(Generosity),avg(DystopiaResidual) FROM world_happiness WHERE HappinessScore > 0 GROUP BY Region;")
	c.execute("SELECT * FROM region_happiness ORDER BY HappinessScore;")
	conn.commit()
	conn.close()





populate_database('world_happiness_report.db','2015.csv')
#clean_database('world_happiness_report.db')
group_happiness_by_region('world_happiness_report.db')