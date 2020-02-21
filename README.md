Gia-Bao HUYNH | [LinkedIN](https://www.linkedin.com/in/gbh198/) | In a winter day | [UDACITY Data Engineering NanoDegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027)

# **CLOUD DATA WAREHOUSE USING S3 AND REDSHIFT**

## Table of Contents
-	Preamble
-	Introduction
-	Data Source
-	Setting Up Your AWS
- Schema Design
-	Project Template
-	How To Use
- Data Warehouse Optimization (Optional)

## Preamble

This repo is my work for the Cloud Data Warehouse project in UDACITY [Data Engineering NanoDegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027). Thank you Udacity team for all the support you have provided. 
## Introduction
As migrations to Cloud are becoming mainstream, Sparkify, a music streaming startup, has decided to store and process their data onto the cloud.

The idea of construction of a data warehouse for Sparkify, besides available databases, is to consolidate data from many sources to create a company’s “single source of truth” that would potentially prevent many disputes derived from analytic workloads. Hence, this Data Warehouse, designed to be read-oriented, strongly serves for OLAP transactions to enhance decision-making efficiency in terms of time and efforts. 

It is my job, as a data engineer, to build ETL pipelines that **Extract** data from data sources, then **Transform** them into a pre-defined schema, and finally, **Load** all to the Cloud Data Warehouse. In this project, for simplicity, I am not going to discuss many versions of Data Warehouse architectures. 
AWS technologies in-use to build this Cloud Data Warehouse are: [Amazon S3](https://aws.amazon.com/s3/)  and  [Amazon Redshift](https://aws.amazon.com/redshift/). 
In this context, Data Source is S3, pre-defined schema is [Star Schema](https://en.wikipedia.org/wiki/Star_schema) and Data Warehouse is hosted by Redshift.
## Data Source
Data sources, a subset of real data from the [Million Song Dataset](http://millionsongdataset.com/), are comprised of two S3 buckets. Two S3 buckets are used to separate two types of available data: **Song Data** (*info about songs and artists*), **Log Data** (*info about user’s activities*). 
- Song data: s3://udacity-dend/song_data
- Log data: s3://udacity-dend/log_data
- Log data JSON path: s3://udacity-dend/log_json_path.json

*This Log data JSON path file recursively goes through all the paths in folder to extract data.*

All the files available in these two S3 buckets are in JSON format. For example:
- song_data/A/B/C/TRABCEI128F424C983.json
- song_data/A/A/B/TRAABJL12903CDCF1A.json

And below is an example of what the data in a log file, 2018-11-12-events.json, looks like.

![Image of Yaktocat](https://github.com/gbh198/Cloud-Data-Warehouse-With-Amazon-S3-and-Redshift/blob/master/log-data.png)

## Setting up your AWS

The most and foremost thing to remember is that, the mode of using AWS is essentially renting its services to meet our demands. Therefore, cost of renting plays an important role for any decision of using AWS. 
**Do not forget to kill your Cluster once you finish project.**

**1. Working environment**

It is possible to choose a working environment that suits us best. In most of cases, administering AWS using Management Console is preferable. 
Sometimes, [Insfrastructure as Code (IaC)](https://en.wikipedia.org/wiki/Infrastructure_as_code) is more convenient for Developers. 
If so, Python3 + AWS SDK [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html) libraries and their dependencies are good ways to go.

**2. Authorisation**

S3 never knows which client is approaching. This is the reason why [IAM role](https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles.html) authorization mechanism is critical for S3 to know client’s identity and which policy that client could conduct on him. In this project, policy attached to this IAM role is [AmazonS3ReadOnlyAccess](https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies_create.html).

**3. Redshift Set-up**

Redshift is where Data Warehouse is hosted and also a place where data will be ingested after transformation.
I decided to configure in cost efficient mode as follow:

- **Cluster**: 4x dc2.large nodes. $0.25/node/hour. 160 GB SSD Storage per node.
- **Location**: US-West-Oregon.

The choice of US-West is made because of the location of S3 buckets (it should be declared in COPY commands as 'region'). This a good practice to increase speed of ingesting data. 

## Schema Design

Sparkify Date Warehouse schema has a star design. This design is optimal for ad-hoc queries and understandable for users. 
Star design means that it has one Fact Table surrounded by Dimension Tables. 
The Fact Table will answer a business question. In case of Sparkify, Fact Tables is about: **“What songs are users listening to?”**.

There are also two staging tables:  One for SONG data and another for LOG data. Staging stables is the destination of COPY commands and, at the same time, the departure of INSERT commands. 

## Project Template
 
-	**create_tables.py** – Dropping old tables and Creating new ones
-	**etl.py** – ETL process to Extract data from S3, Transform and Load to Redshift
-	**sql_queries.py** – Modular collection of SQL queries (grouped by functions)
-	**dhw.cfg** - Configuration file for setting up Redshift & S3

## How To Use
**Required libraries**
- configparser
- psycopg2
- ipython-sql (if run Jupyer Notebook)

1.	Setting your file system to root, navigate to the folders that contains python files, and execute following command in terminal:
**python create_tables.py** 
2.	Running your ETL process :
**python etl.py** 
3.	Running the analytic queries on your Redshift database to compare your results with the expected results.
4.	Delete your Redshift cluster as soon as you finish.

**P/s**: If connection to Redshift is not established, it could be caused by wrong ENDPOINT address. Furthermore, be sure that the privacy mode of your Redfshift Subnet Group is set to public.

## Data Warehouse Optimization (Optional)

1. For processing stage, conceptions of [Distribution Key](https://docs.aws.amazon.com/redshift/latest/dg/c_best-practices-best-dist-key.html), Sorting Key and Distribution Style of Redshift could potentially help increase speed of processing. In this demo, as data set is small enough, I decided to distribute copies of 5 dimensions tables to all nodes.
2. Besides above-mentioned tables, [Materialized Views](https://www.postgresql.org/docs/9.3/rules-materializedviews.html) could be created to improve speed of frequently-executed queries.
3. [Constraint Triggers](https://www.postgresql.org/docs/9.0/sql-createconstraint.html)

