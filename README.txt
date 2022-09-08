Manifold Data Processing Application
Author: Jeff Witmer
Last Updated: September 6, 2022
Created for Manifold Inc. 

About
-----

The Manifold Data Processing Application reads a dataset stored as individual JSON files, processes the data, writes the results to a structured data set, and displays the results of some data analysis. 

How to Run
----------

1. Add your data to the manifold/data folder
2. Ensure Docker is running
3. Execute make build
4. Execute make process to process the data
5. The processed data is saved in the processed_data.csv file
6. Analysis results are displayed in the console

Design
------ 
1. Identify all JSON files in the data folder
2. For each file:
   1. Read each data file as a DataFrame, one at a time
   2. Determine which records should be processed and which should be skipped
   3. Perform analysis and save the results in memory 
   4. Write the processed records to disk
3. Print final analysis results to console

Known Issues and Considerations
-------------------------------

Data Loading:
* The program requires the JSON files to be in a standard and consistent format. The program is not adaptable to JSON files in a different storage format.

* While memory efficient, only keeping a single data in memory at a time means the analysis needs to be implemented in a loop. It may be difficult to add more complex analyses in the future. This design choice also has readability and maintainability ramifications. 

* No schema for the column types of the data is used. This causes zip_code to be treated as a numeric data type when it should probably be treated as a character. 

* Data is only read from the data folder. User should be able to provide a path to the data without having to move it to a folder in the app. 

Data Output:
* Improvement: Write the data to a parquet file or distributed database. Conduct additional analysis using Spark. While this may be more memory efficit, it could be more cumbersome if the input data is relatively small.

* Analysis results are simply printed to the console. Depending on the use case, results would likely need to be saved. 

Docker:
* The provided skeleton docker-compose. For simplicity, I just used docker. 

* Currently, the unit tests are not run in a Docker container. This may cause unexpected errors when users other than me attempt to run the tests. Using docker-compose, a multi-stage Docker build, or multiple DockerFiles, make test should be updated to run in a Docker container. 

Unit Tests: 
* More units are needed to ensure the code functions as intended. The current unit tests only show the basic skeleton for how unit tests could be set up.