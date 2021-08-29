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

1. Read the airports data and Show the top 10.
2. Read the flights data and Show the top 10.
3. Read the airlines data and Show the top 10.
4. Count the number of row and columns of flights dataset.
5. Select the columns: AIRLINE, FLIGHT_NUMBER, TAIL_NUMBER, ORIGIN_AIRPORT, DESTINATION_AIRPORT, SCHEDULED_DEPARTURE from flights.
6. Select the flights whose Air time is greater than 120 minutes (2 hr).
7. Show the airlines available between Airports based on the IATA codes of origin and destination airport.
8. Select the flights whose Origin airport is "SEA" and Destination airport is "PDX".
9. Calculate the duration of flights in hours and show top 10.
10. Find the average speed of each flights in Km/hr and display 'TAIL_NUMBER', 'ORIGIN_AIRPORT', 'DESTINATION_AIRPORT' and 'average speed'.
11. Find the shortest distance in Km, covered by the flight that originates from airport 'PDX'.
12. Find the longest flight from SEA in terms of air time.
13. Group the flights by its tail number and find the Number of flights per airplane.
14. Group the flights by origin airport and calculate the average time of the flights from SEA to PDX.
15. Group the data by month and destination airport and find the Average departure delay of the flights.
16. Join the DataFrames airports and flights based on the IATA codes of the origin airport.
17. Display MONTH, DAY OF WEEK, AIRLINE, TAIL NUMBER, DESTINATION_AIRPORT, AIR_TIME, DISTANCE and ARRIVAL_DELAY form flights where ARRIVAL_DELAY, AIRLINE, AIR_TIME,     TAIL_NUMBER is not NULL.
18. Display flights with a delay and create a new column ('label') that shows the labels assigned to the flights, where 0 indicates no delay and 1 indicates delay.
19. Count the number of flights that has delay (arrival time > 0 minutes), no delay and null labels.
20. Count the number of flight from each of the combinations of origin Airport and destination Airport.
21. Join flight and airlines data based on IATA codes of the airports inorder to display the abbrevations of the airlines.


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
 




