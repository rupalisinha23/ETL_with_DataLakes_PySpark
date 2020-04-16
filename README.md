# ETL with PySpark and Data Lakes in AWS

In this project, we buit an ETL pipeline for a data lake hosted on S3. Following steps were performed:
- Load data from S3
- Process the data into analytics tables using Spark
- Load the processed data back to S3

## Project Motivation
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

## Why Data Lake?
- All data is loaded from source systems. No data is turned away.
- Data is stored at the leaf level in an untransformed or nearly untransformed state.
- Data is transformed and schema is applied to fulfill the needs of analysis.
- Supports all data types.

## Data Modelling
For the project, <a href="https://en.wikipedia.org/wiki/Star_schema"><b>Star Schema</b></a> is created as shown in the image.
![fact and dimension tables](img/table.png)

## Getting Started
### Pre-requisites
- Python 3.5 and above --> For more details https://www.python.org/
- Account in AWS --> For more details https://aws.amazon.com/what-is-aws/
  - Create an IAM user --> https://docs.aws.amazon.com/IAM/latest/UserGuide/id_users_create.html
  - Get your SECRET KEY and ACCESS KEY and save it in your local machine --> https://docs.aws.amazon.com/general/latest/gr/aws-sec-cred-types.html
  
### Running the project
<b>STEP 1:</b>  Open dl.cfg file and set KEY and SECRET with your AWS ACCESS KEY and AWS SECRET KEY respectively. <br>
<b>STEP 2:</b> Create and run the environment
```
$ python3 -m venv virtual-env-name
$ source virtual-env-name/bin/activate
$ pip install -r requirements.txt
```
<b>STEP 3:</b> Run the below command
```
$ python etl.py
```

<b>Note: </b><i>We are given with only the input s3 bucket's address and according to the project requirements, we are supposed to create the output s3 bucket first and then run the ETL pipeline. I would like to specify that this has been taken care off in the etl.py. The code simply creates the output bucket if it does not exists. You simply have to provide/change the name in dl.cfg file. </i>


### Project Structure

:paperclip: dl.cfg: configuration file to store the AWS key and secret key along with the address of s3 input and output bucket <br>
:paperclip: etl.py: code for the etl pipeline <br>
:paperclip: utils.py: function to create the aws output s3 bucket <br>
:paperclip: requirements.txt <br>
:paperclip: README.md <br>
:paperclip: .gitignore 

### Authors
* **Rupali Sinha** - *Initial work*

