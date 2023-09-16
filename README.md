# Stack Overflow User Behavior Analysis

## Project Overview

The "Stack Overflow User Behavior Analysis" project is a comprehensive data analysis initiative focused on understanding the behavior and habits of users on the Stack Overflow platform. This project leverages Apache Spark for data processing and MongoDB for data storage.

## Project Tasks

### Task 1: Data Import into MongoDB

- Transfer data from CSV files into MongoDB collections.
- Use the following command to import data from `Questions.csv` and `Answers.csv` into MongoDB collections:

Answer data preview:

![image](https://github.com/huy1741/stackoverflow-spark/assets/64857328/694336f6-6891-42ca-bd0a-6379e880f8e7)



Question data preview:

![image](https://github.com/huy1741/stackoverflow-spark/assets/64857328/a8ce3696-ce2b-4399-8d4c-7f271d41bb8a)


```shell
mongoimport --type csv -d <database> -c <collection> --headerline --drop <file>
```
Replace <database> with the target database name and <collection> with the desired collection name.

### Task 2: Read Data from MongoDB with Spark

- Read data from MongoDB collections into Spark DataFrames.
- Configure the Spark Session with MongoDB connector settings.
- Use the spark.read method to read data from MongoDB collections into Spark DataFrames.

### Task 3: Data Normalization
- Convert date columns (CreationDate and ClosedDate) from String to DateType.
- Handle cases where OwnerUserId has a value of "NA" by converting it to null and changing the data type to IntegerType.

### Task 4: Count Programming Language Mentions
- Count the occurrences of specific programming languages in question text.
- Programming languages to count: Java, Python, C++, C#, Go, Ruby, Javascript, PHP, HTML, CSS, SQL.

![image](https://github.com/huy1741/stackoverflow-spark/assets/64857328/e934d6e0-47ef-4c72-9126-6b39716fc491)

### Task 5: Find Most Used Domains in Questions
- Identify the top 20 domains most frequently referenced in questions.
- Extract domains from URLs mentioned in questions.

![image](https://github.com/huy1741/stackoverflow-spark/assets/64857328/12383c2c-9a4f-4a8c-85d0-03857598ff30)

### Task 6: Calculate Daily Total Score for Users
- Compute the total score achieved by each user on a daily basis.
- Utilize windowing and aggregation operations to calculate daily total scores.

![image](https://github.com/huy1741/stackoverflow-spark/assets/64857328/5f4dde23-5100-4b46-bea1-422cae91bd57)

### Task 7: Join answers and questions
- Join both dataframe for better insights
- Use bucket to improve query performance on large datasets

![image](https://github.com/huy1741/stackoverflow-spark/assets/64857328/e654a66d-528a-4577-8cb1-42fb49a133e4)




