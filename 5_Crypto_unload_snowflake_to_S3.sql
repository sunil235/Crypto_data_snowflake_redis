----Unload data from Snowflake to AWS s3(csv compressed) 
--Steps

1.Under aws IAM, create snowflake policy (eg. snowflake_access)   and under {} JSON  and paste this : {replace "cryptoresults" with appropriate bucket name}

{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "s3:PutObject",
                "s3:GetObject",
                "s3:GetObjectVersion",
                "s3:DeleteObject",
                "s3:DeleteObjectVersion"
            ],
            "Resource": "arn:aws:s3:::cryptoresults/*"
        },
        {
            "Effect": "Allow",
            "Action": [
                "s3:ListBucket",
                "s3:GetBucketLocation"
            ],
            "Resource": "arn:aws:s3:::cryptoresults",
            "Condition": {
                "StringLike": {
                    "s3:prefix": [
                        "*"
                    ]
                }
            }
        }
    ]
}


2.Under aws IAM ,create role (eg. snowflake_role) under "Another AWS account,3rd party".get accountid from aws login user dropdown.Check external ID and set 0000 temporarily.
 Next for permissions , select snowflake_access and finish

3.create aws user under IAM to get security tokens.
  Add permissions : AmazonS3FullAccess and snowflake_access 

4. Under snowflake worksheet , create integration object, 

--AWS S3 configuration
-- Use accountadmin role
create or replace storage integration s3_int
  type = external_stage
  storage_provider = s3
  enabled = true
  storage_aws_role_arn = 'arn:aws:iam::<your aws account id>:role/<snowflake role created above>'
  storage_allowed_locations = ('s3://<aws bucket name>/');
  
5.  
desc integration s3_int 

Get STORAGE_AWS_IAM_USER_ARN and STORAGE_AWS_EXTERNAL_ID from above desc sql

6.Edit this under AWS snowflake role in trust relationship  and replace 0000 with captured STORAGE_AWS_EXTERNAL_ID and pricipal:AWS with captured STORAGE_AWS_IAM_USER_ARN

7.Now , connection is binded between AWS and snowflake . create file format , stages and alter integration object to add more allowed list

Eg.

create or replace file format my_csv_file_unload_format
type = csv field_delimiter= ',' skip_header = 1 null_if = ('NULL','null') empty_field_as_null = true compression = gzip;


alter storage integration s3_int
set storage_allowed_locations = ('s3://cryptoresults/','s3://cryptoresults/crypto_main_unload/');


create or replace stage my_s3_unload_stage
storage_integration = s3_int
url = 's3://cryptoresults/crypto_main_unload/'
file_format = my_csv_file_unload_format;


8.Create snowflake stored proc ( javascript language) to create a dynamic file with timestamp suffix to unload data from snowflake table to S3.Example :

create or replace procedure crypto_main_unload_proc()
returns string not null
language javascript
as
$$

var today = new Date();
var date = String(today.getFullYear())+String("00"+today.getMonth()+1).slice(-2)+String(today.getDate());
var time = String(today.getHours()) + String(today.getMinutes()) + String(today.getSeconds());
var dateTime = date+time;

var cmd = `copy into @my_s3_unload_stage/`+ dateTime + `/ from (select * from crypto_main order by id) overwrite=true;`;

try {
     var sql = snowflake.createStatement({sqlText: cmd});
     var rs = sql.execute();
     result = "Succeeded!Rows Unloaded";
  }
 catch (err) {
    result = "Failed: Code: " + err.code + "\n State: " + err.state;
    result = "\n Message: " + err.message;
    result = "\n Stack Trace:\n" + err.stackTraceTxt;
 }

return result;
$$;




