# Analyze Airlines data

## Project Description

Project 2 where we are working in small groups to develop an application that willprocess and extract value from large Airlines datasets in Spark and apply what we have learned utilizing the differentoptimizations and components of Spark such as DataFrames, Datasets, SparkSQL and RDD's
## Technologies Used

* Hadoop - version 2.7.3.2.6.5.0-292
* HDFS - version 2.7.3.2.6.5.0-292
* spark -version 2
* Git -version 2.32.0.windows.1

## Features

List of features ready and TODOs for future development
using this project some query are solved using pyspark,dataframe ,following questions are listed below:

01. Read in the airports data and Show the top 10.
02. Read in the flights data and Show the top 10.
03. Read in the airlines data and Show the top 10.
04. Count and columns of flights dataset.
05. Select the columns AIRLINE, FLIGHT_NUMBER, TAIL_NUMBER, ORIGIN_AIRPORT, DESTINATION_AIRPORT, SCHEDULED_DEPARTURE FROM flights.
06. Select the flight whose Air_time >120.
07. Show the airlines available between various Airport.
08. Select the flight whose Origin_airport is "SEA" and Destination_port is "PDX".
09. Add duration_hrs columns and show it.
10. Find the average speed of each flights.
11. Find the shortest flight from PDX in terms of distance.
12. Find the longest flight from SEA in terms of air time.
13. Group the flights by tailnum and find Number of flights each plane made.
14. Group the flights by origin_airport and Average duration of flights from PDX and SEA.
15. Group the data by month and dest and find Average departure delay.
16. Join the DataFrames airports and flights.
17. FILTER (CHOSEN) COLUMSN WITHOUT NULL.
18. FLIIGHTS WITH A DELAY Creating is_late (label).
19. FIND FLIGHTS WITH DELAY, NO DELAY, NULL (print('Labels distrubution:')).
20. Count the number of flight from origin_Airport to Destination_Airport.
21. Merge flight and airlines data based on airport [SQL].



## Getting Started
   

To start this project user need to install sandbox-Hortonworks in virtual machine.After installing the VM start the VM then put the following command in Git bash then connect to VM using SSH command "ssh maria_dev@sandbox-hdp.hortonworks.com -p 2222" after this you need to put the password the default password for USER maria_dev is "maria_dev"

Performing the above action you are enter to VM CLI then do the following command which are listed below for the project.

* Create a folder in local VM using command "mkdir folder_name"
* In this folder clone the git repository from where we pull the dataset using command "git clone 'git repository link' "
* All dataet are in a zip file we have to unzip it using command "unzip file_name" (N.B: if you are unzip in the current file ) .
* After unzip all the file create a folder in hdfs using command which is little bit difference before we tried while creating folder is "hdfs dfs -mkdir folder_name" (By default this folder created in the path user/maria_dev/ ) and copy all file to the hdfs folder by ""hdfs dfs -put file-name /user/maria_dev/folder_name".
* Also a second procedure is there by which we can add file from local machine to our VM using command "scp -P 2222 ./airlines.csv  maria_dev@sandbox-hdp.hortonworks.com:/home/maria_dev/Project_2"
* Before performing all the above action try to check whether your Ambari which is management platform for hadoop started all services or not.
* After performing all the action try to create a .py file where write all the program using "vi" editor and save the file using esc + : + wq .
* After saving the file give command "spark-submit file_name" it will execute the file using SPARK 2 version.

## Usage

>Using this project any one can perform analysis with the Airlines dataset
 




