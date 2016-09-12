# python-code

###mbackup.py 
mbackup.py is a python program that copies a directory structure from local filesystem to an S3 endpoint and preserves permissions when moving data to s3 through local tags.  It optimizes for performance by specifiying the number of threads moving data from source filesystem to s3 endpoint. Auth is provided through a file named igauth located in the <HOMEDIR>/.aws/ directory. Profiles can be specified in the igauth file to use for backup.

Requirement:  python3, boto3, botocore, argparse, configparser

See file for dependanices and configuration. 

### backupmain.py 
backupmain.py - initial version of mbackup without as much fail and retry logic. 
