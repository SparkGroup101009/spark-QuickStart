# spark-QuickStart

Follow Steps to execute spark job

Prerequisites
1. Java 8
2. Maven
3. Spark

## Step 1 
  Install spark if you want to submit spark job
  
  Download Spark latest version from https://spark.apache.org/downloads.html
  
  Extract file in below location
  **/usr/local/spark**
  
  Set environment variables path of spark bin directory in *.bashrc* file
  ```
  export PATH=${PATH}/usr/local/spark/bin
  ```
## Step 2
  Execute below maven command to compile code and downlaod required dependencies
   ```
  mvn clean install
   ```
## Step 3
   Execute below maven command to copy all dependencies into an common location, below command copy all dependencies in to location **/home/ist/Desktop/lib/**
   ```
   mvn dependency:copy-dependencies -DoutputDirectory=/home/ist/Desktop/lib/
   ```
## Step 4
   Execute below maven command to submit spark job
   ```
   java -cp /home/ist/Desktop/lib/*:./sparkLearning-0.0.1-SNAPSHOT.jar: org.aminfo.spark.sparkLearning.WordCount
   ```
