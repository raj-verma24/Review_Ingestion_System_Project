**REVIEW INGESTION SYSTEM**

Target is to implement a Restaurant Review Ingestion System which can derive useful insights and informations from a customer's review about a restaurant.

**Scenario:

You are working for a company that specializes in restaurant reviews and are tasked with designing the new Review Ingestion System.

### Requirements

The reviews we receive might contain inappropriate words, so we need to make sure that we filter these words out and replace them with asterisks (`****`). If a review contains too many inappropriate words in proportion to the overall text, we consider the entire review as inappropriate and need to filter it out. Furthermore, we are not interested in outdated reviews, as they might no longer reflect the reality of the restaurant today.

The Product department requests that we deliver the following aggregated metrics per restaurant ID:
* Number of reviews
* Average score
* Average length of review
* Review age (in days)
  * Oldest
  * Newest
  * Average


## Framework used for the implementation

**Cloud Implementation (Task 1)

I have used GCP environment to design the architecture for task 1, you can find all the details about the implementation in a ms-word file (Design Document for Review Ingestion System).
you will find below key points in the document :

1. Design Architecture 
2. Data Flow Diagram
3. Solution Overview
4. Process Overview
5. Detailed Design
6. Scaling Strategies
7. Deployment Strategies
8. Conclusion

**Local implementation (Task 2)

I have used pyspark framework for implementation which can provide distributed data processing architecture capable of handling huge datasets with ease. Although the solution is to run the code in single machine but we can use the same framework for to process large dataset in multi cluster environment.

This file contains all the information about the underline folders and files which i have used and the steps required right from setting up run environment to executing code in local environment.


##Setting up the environment

Setting up PySpark on a Windows laptop requires a few steps, including installing Python, Java, Hadoop (for the winutils utility), and Spark itself. Here's a complete guide to get PySpark running on your Windows machine.

**Step 1: Install Python**
Download and Install Python from the official website: Python.org

Choose the latest stable version (Python 3.x).
During installation, make sure to check the box "Add Python to PATH".
Verify Python Installation: Open a command prompt and run:

python --version
pip --version

**Step 2: Install Java (JDK)**
PySpark requires Java to run, so you'll need to install the Java Development Kit (JDK).

Download and Install JDK:

Download the latest JDK from Oracle's official site: Oracle JDK (use JDK 8 or 11).
Install the JDK and note the installation path.
Set Up Java Environment Variables:

**Step 3: Install Hadoop winutils (Windows Utility)**
Since Spark on Windows needs winutils.exe, you'll need to install Hadoop binaries for Windows.

Download winutils.exe and set up HADOOP_HOME Environment Variable

Go to the Winutils Repository and download the appropriate version of Hadoop (e.g., 2.7.1).
Extract the contents and place it in a directory, for example: C:\hadoop.
Set Up HADOOP_HOME Environment Variable:

Open System Properties > Environment Variables.
Add a new System Variable:
Variable name: HADOOP_HOME
Variable value: C:\hadoop (the path where you extracted Hadoop).
Add winutils to PATH:

Go to the Path variable under System Variables, click Edit, and add C:\hadoop\bin.

**Step 4: Download and Install Spark**
Download Apache Spark:

Go to Spark Downloads.
Choose a Spark version (latest stable version), and under "Pre-built for Apache Hadoop", choose the Hadoop 2.7 version.
Download and extract the Spark files to a directory, e.g., C:\spark.
Set SPARK_HOME Environment Variable:

Open System Properties > Environment Variables.
Add a new System Variable:
Variable name: SPARK_HOME
Variable value: C:\spark (the path where you extracted Spark).
Add Spark to PATH:

Go to the Path variable under System Variables, click Edit, and add %SPARK_HOME%\bin.

**Step 5: Install PySpark**
Open a command prompt and run:

pip install pyspark

Verify the installation by running:

type pyspark in command line bash shell
If everything is set up correctly, it should open the PySpark shell.


##Understanding the files and folders

You will find below files inside project directory

INPUT FILES
1. review.jsonl -- json format file containes all customer reviews (you can choose your own review.json file)
2. inappropriate_words.txt - contains all inappropriate words which we want to filter out from the review comments. (you can add more inappropriate words in that file)
3. review_processor.py - python file which has the implementation logic in pyspark framework

OUTPUT
Valid_review folder - This is the folder in which our valid reviews.jsonl output file will be created.
aggregations folder - This is the folder where final aggregated data will be written.
Discarded_review folder - This is the folder where discarded reviews will be written in jsonl file format

Note:you don't need to create any output folder, it will be created automatically.

##Steps of Execution

1.open command prompt
2. cd to the path where you have kept all the input files required.
3. trigger below command to run the python file with required parameter.

*spark-submit review_processor.py --input reviews.jsonl --inappropriate_words inappropriate_words.txt --output valid_reviews.jsonl --discarded discarded_reviews.jsonl --aggregations aggregations.jsonl




