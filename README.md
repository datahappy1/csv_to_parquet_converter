# csv_to_parquet_converter

## to install and run
1) pip3 install -r requirements.txt
2) in case you wish to use AWS S3 as a source file or a target file for the conversion,
set environment variables like:
`aws_access_key_id = <your AWS IAM access key id>`
`aws_secret_access_key = <your AWS IAM secret access key value>`

Pandas uses s3fs to integrate with AWS S3, please see in case of any concerns.
3) setup a virtual environment
4) run like:
`python __main__.py`

The arguments need to be set like:
##for csv to parquet conversion:

###local csv file to local parquet file:

-sfp C:\<your local folder>\source.csv
-tfp C:\<your local folder>\target.parquet

###local csv file to s3 parquet file:

-sfp C:\<your local folder>\source.csv
-tfp s3://<your bucket name>/<your bucket "folder" prefix>/target.parquet`

###s3 csv file to local parquet file:

-sfp s3://<your bucket name>/<your bucket "folder" prefix>/source.csv
-tfp C:\<your local folder>\target.parquet

###s3 csv file to s3 parquet file:

-sfp s3://<your bucket name>/<your bucket "folder" prefix>/source.csv
-tfp s3://<your bucket name>/<your bucket "folder" prefix>/target.parquet`

##for parquet to csv conversion:

###local parquet file to local csv file:

-sfp C:\<your local folder>\source.parquet
-tfp C:\<your local folder>\target.csv

###local parquet file to s3 csv file:

-sfp C:\<your local folder>\source.parquet
-tfp s3://<your bucket name>/<your bucket "folder" prefix>/target.csv`

###s3 parquet file to local csv file:

-sfp s3://<your bucket name>/<your bucket "folder" prefix>/source.parquet`
-tfp C:\<your local folder>\target.csv

###s3 parquet file to s3 csv file:

-sfp s3://<your bucket name>/<your bucket "folder" prefix>/source.parquet`
-tfp s3://<your bucket name>/<your bucket "folder" prefix>/target.csv`



### to verify a parquet file:
http://parquet-viewer-online.com/
