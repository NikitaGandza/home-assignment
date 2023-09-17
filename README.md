# home-assignment

Run Posgres container 
Add main.py, Dockerfile, .env, requirements.txt to the same directory and use these commands to build and run ETL container:

docker build -t my-first-container . 
docker run --net=host my-first-container

Container is using main.py script for ETL process and creates processed_df.csv file, whis is also stored in this repository. In addition, I've added an query.sql, which contains SQL query, to achive same result.

Thanks for the task!
