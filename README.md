# Project: Data lake
 
A music streaming startup, _Sparkify_, has grown their user base and song database and 
want to move their processes and data onto the cloud. Their data resides in S3, in a 
directory of JSON logs on user activity on the app, as well as a directory with JSON 
metadata on the songs in their app.

In order to help _Sparkify_ to continue digital analytics smoothly despite rapid growth
of their customer and song database, I created a data lake solution for data storage
which can keep their data organized and can make it easily accessible for reporting
and analytics.

1. Sparkify data were transferred from AWS/S3 bucket into two spark dataframes, 
**df_events_raw** and **df_songs_raw**. In order to save time and money on expensive 
   Amazon EMR managed cluster platform, only part of the song data were processed. 
2. The proposed schema for the data structure is the **Star schema** 
![Star schema](ERD_sparkifydb.jpg "Star schema") 

   - The **songplays** is a prototype of a fact table.
   - Prototype of dimension tables: **users**, **songs**, **artists**, **time**

3. Processed data in parquet format were uploaded into another AWS/S3 bucket. Each 
   table has its own folder within the directory. **Songs** table files are partitioned 
   by year and then artist. The **time** table files are partitioned by year and month. 
   **Songplays** table files are partitioned by year and month.

## The implementation is designed to run in the following order.
All scripts should be run in the **terminal window**. Make sure that aws-cli is installed 
on your local computer: `aws --version`. If it's not installed then run: 
`pip3 install awscli --upgrade --user`. Note that EMR cluster and S3 bucket are in the 
region: `us-east-2`.

1. Download all files from **Project Workspace** or from 
   `s3://course-datalake-prj/data-lake-project/` using following command:
   - ```aws s3 cp  s3://course-datalake-prj/data-lake-project/ data-lake/```
   - all the following commands should be run from **data-lake** directory.
   - Create you own AWS key pair with name **my-emr-cluster**. It is assumed that this file will 
     be in **data-lake** directory.

2. Optional steps: 
   - Fill in your **aws_access_key_id** and **aws_secret_access_key**
   - Run `./aws_cli_setup_extra.sh ` script and just press "Enter" on each prompt. 
   The script will set appropriate AWS credentials.
   
3. Launch EMR cluster `./aws_emr_cluster.sh ` and check the status using the following 
   command:
   ```aws emr describe-cluster --cluster-id <cluster-id-shown-after-script-execution>```
   then re-run the command until you see status "WAITING". This command also provide 
   information about "MasterPublicDnsName". 
   - For example, "ec2-18-116-231-150.us-east-2.compute.amazonaws.com". 
   - Upload files to master node:
     ```
     sftp -i my-emr-cluster.pem hadoop@ec2-18-116-231-150.us-east-2.compute.amazonaws.com
     ```
     - after successful connection via sftp use `put  dl.cfg` and `put  dl_etl.py`
   - The following command allows connecting to your cluster: 
     ```
     ssh -i "spark-cluster.pem" -N -D 8157 hadoop@ec2-18-116-231-150.us-east-2.compute.amazonaws.com
     ``` 
     - after successful connection via ssh use `spark-submit  dl_etl.py` to start spark job. 
     - After job is complete, processed files will be uploaded to 
       `s3://course-datalake-prj/analytics/`. 
4. To terminate cluster run: 
``` 
aws emr terminate-clusters --cluster-id <cluster-id>
```
5. To test ETL locally in the **Project Workspace** run all cells in `dl_etl.ipynb` 
   - Processed file will be written locally to `/home/workspace/analytics/`

