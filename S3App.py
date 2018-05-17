import boto3
from boto3.s3.transfer import S3Transfer
import configparser
import os

try:
    step_msg        = "Reading config file."
    #Read credentials from *.ini file
    config = configparser.ConfigParser()  
    config.read('s3Config.ini')
    secret_key = config['DEFAULT']['SECRET_KEY']
    key_id = config['DEFAULT']['KEY_ID']
    region = config['DEFAULT']['REGION']
    local_path = config['DEFAULT']['LOCAL_PATH']
    bucket = config['DEFAULT']['BUCKET_NAME']
    s3Files = []

    if not os.path.exists(local_path):#Validate local path
        print ("Directory ("+local_path+"), don't exists")
    else:
        LFiles = os.listdir(local_path)

    #Get s3 resource with config credentials 
    s3 = boto3.resource('s3', aws_access_key_id=key_id, aws_secret_access_key=secret_key, region_name=region)
    step_msg = "Get s3 resource with config credentials."
    S3bucket = s3.Bucket(bucket)#Get bucket

    if S3bucket.creation_date is None: #Validate bucket exists
        print("Bucket ("+bucket+"), don't Exist")
    else:
        s3cli = boto3.client('s3')#Get s3 client
        step_msg = "Get files on bucket."
        for files in S3bucket.objects.filter(Prefix=""): #Get files on bucket
            if "." in files.key and not "/" in files.key: s3Files.append(files.key)
        
        #Upload/update files on bucket
        for srcFile in LFiles: 
            transfer = S3Transfer(s3cli)
            
            if srcFile in s3Files:
                step_msg = "uploading files on bucket. (New)"
                filename = os.path.basename(local_path+"\\"+ srcFile)
                transfer.upload_file(local_path+"\\"+ filename, bucket, srcFile)
                print('File ' +srcFile+' uploades.')
            else:
                step_msg = "uploading files on bucket. (Update)."
                transfer.upload_file(local_path+"\\"+ filename, bucket, srcFile)

        #Delete files on bucket
        for trgFile in s3Files:
            
            if trgFile not in LFiles:
                forDeletion = [{'Key':trgFile}]
                step_msg = "Deleting files on bucket."
                s3cli.delete_objects(Bucket=bucket, Delete={'Objects': forDeletion})
                print('File ' +trgFile+' deleted.')

except Exception as e:
        print("Error on step: "+step_msg +"\n Message: "+str(e))