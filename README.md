# Stack Overflow User Behavior Analysis

## Project Overview

The "Stack Overflow User Behavior Analysis" project is a comprehensive data analysis initiative focused on understanding the behavior and habits of users on the Stack Overflow platform. This project leverages Apache Spark for data processing and MongoDB for data storage.

## Project Tasks

### Task 1: Data Import into MongoDB

- Transfer data from CSV files into MongoDB collections.
- Use the following command to import data from `Questions.csv` and `Answers.csv` into MongoDB collections:
Answer data preview:

![image](https://github.com/huy1741/user-behavior-spark/assets/64857328/cd9d29a1-1463-45fe-944e-f610a354cd9c)

Question data preview:

![image](https://github.com/huy1741/user-behavior-spark/assets/64857328/b5e338d7-a43b-4e37-be30-0dc8378f7306)

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

![image](https://github.com/huy1741/user-behavior-spark/assets/64857328/c4533608-2789-4b55-97df-2fb1502edc35)

### Task 5: Find Most Used Domains in Questions
- Identify the top 20 domains most frequently referenced in questions.
- Extract domains from URLs mentioned in questions.

![image](https://github.com/huy1741/user-behavior-spark/assets/64857328/a8e2e8ca-cadc-466a-97d5-71e4c2d0b35b)

### Task 6: Calculate Daily Total Score for Users
- Compute the total score achieved by each user on a daily basis.
- Utilize windowing and aggregation operations to calculate daily total scores.

![image](https://github.com/huy1741/user-behavior-spark/assets/64857328/ab99a81a-95d6-476a-8896-3e29fe1e6284)


