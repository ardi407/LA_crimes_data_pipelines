-- Nama		: Ardianto
-- Batch	: RMT - 024

-- Dalam file Sql ini, dilakukan beberapa proses seperti pembuatan database, pembuatan table, dan copy isi table dari file csv.

-- Pembuatan database
CREATE DATABASE airflow;


-- pembuatan table
CREATE TABLE table_m3(
	 "DR_NO" int,
	 "DATE OCC" varchar,
	 "TIME OCC" time,
	 "AREA" int,
	 "AREA NAME" text,
	 "Rpt Dist No" int,
	 "Crm Cd" int,
	 "Crm Cd Desc" text,
	 "Mocodes" varchar,
	 "Vict Age" int,
	 "Vict Sex" varchar,
	 "Vict Descent" varchar,
	 "Premis Desc" text,
	 "Weapon Used Cd" decimal,
	 "Weapon Desc" text,
	 "Status" text,
	 "Status Desc" text,
	 "LOCATION" text,
	 "LAT" decimal,
	 "LON" decimal
);


-- Copy data
COPY table_m3 
FROM 'D:\DATA SCIENCE\Hacktiv8\Phase 2\Milestone 3\crime_in_la.csv' 
DELIMITER ',' 
CSV HEADER
;
