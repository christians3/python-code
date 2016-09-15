#!/usr/local/bin/python3
#################################################################################################
#MIT License
#
#Copyright (c) 2016 Christian Smith, Igneous Systems
#
#Permission is hereby granted, free of charge, to any person obtaining a copy
#of this software and associated documentation files (the "Software"), to deal
#in the Software without restriction, including without limitation the rights
#to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#copies of the Software, and to permit persons to whom the Software is
#furnished to do so, subject to the following conditions:
#
#The above copyright notice and this permission notice shall be included in all
#copies or substantial portions of the Software.
#
#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#SOFTWARE.
#################################################################################################
#################################################################################################
# Description:  mbackup.py 
# Sample program to backup a directory path to an s3 endpoint. The program will scan through 
# the initial filesystem and push the filenames into a queue.  Then each threadworker will 
# take an filename off the queue and upload it to the S3 endpoint and capture the unix permissions
# associated with the file and tag it to the object.  
#
# Usage:  mbackup.py -n "number of copy threads"  -b "bucketname" -p "profilename"  <backup_directory> 
#
# Requirements :  	python3 
#			boto3 
#    			botocore
#			argparse 
#			configparse 
#
# Installation : 
#     
# 	Create a file named igauth under .aws/ in your home directory 
#
#	Sample contents : 
#
#		[default]
#		aws_access_key_id = <access key>
#		aws_secret_access_key = <secret key>
#		endpoint = http://demo.iggy.bz:7070	
#
#	Note different profiles can be created with different endpoints and access keys 
#
# 
#################################################################################################

## system packages 
import os
import sys
import time
import mimetypes

## Boto Packages 
## pip3 install botocore 
## pip3 install boto3
import boto3
import botocore
from boto3.s3.transfer import TransferConfig
from boto3.s3.transfer import S3Transfer

# for multiprocessing queues
from multiprocessing import Pool, Process, JoinableQueue

# need to install configparser and argparse 
# pip3 install configparser 
# pip3 install argparse 
import configparser
import argparse


#################################################################################################
# mUpload - Initialize the worker thread, grab next item off the queue and call the 
#           upload of the file to s3
#################################################################################################
class mUpload(Process):

	def __init__(self, file_queue, env_variables):
		Process.__init__(self)
		self.file_queue = file_queue
		self.env_variables = env_variables

	def run(self):
		proc_name = self.name

		s3, client, transfer = setup_connection(self.env_variables)

		while True:
			next_task = self.file_queue.get()
			print("{}: Queue String: {}".format(proc_name,next_task))

			if next_task is None:
				print("Exiting: {}".format(proc_name))
				self.file_queue.task_done()
				break

			Upload(self.env_variables['bucketname'], next_task[0], next_task[1], transfer)

		print("{}: Queue String: {}".format(proc_name,next_task))
		print("Exiting {}".format(proc_name))
		return

#################################################################################################
# Fileinfo - extract the mimetype or set the values to null
#################################################################################################
def Fileinfo(full_file_name):

	try:
		mime_type, encode_type = mimetypes.guess_type(full_file_name, False)

		if mime_type is None:
			mime_type = "Unknown"
		if encode_type is None:
			encode_type = "Unknown"

		file_name, extension_name = os.path.splitext(full_file_name)

		if extension_name is None:
			extension_name = ""


	except IOError as e:
		print("I/O error {}:{}".format(e.errorno, e.strerror))
		sys.exit(1)

	return mime_type, encode_type, extension_name

#################################################################################################
# Uploadfile - function to upload the file to an object store with retry logic in the 
#              event of a failure or busy endpoint 
#################################################################################################
def Uploadfile(bucketname, directory, full_file_name, transfer, stat_info, mime_type, encode_type, extension_name):

	full_file_name_woslash = full_file_name[1:]

	if directory is True:
		full_file_name = "/tmp/zerofile.0"

	max_retries = 10
	loop_again = True

	while loop_again is True and max_retries > 0:
		try:
			transfer.upload_file(full_file_name,
					bucketname,
					full_file_name_woslash,
					extra_args={"Metadata":{'backup-date'   : time.ctime(),
								'meta-mode'     		: str(stat_info.st_mode),
								'meta-uid'	  		: str(stat_info.st_uid),
								'meta-gid'	    		: str(stat_info.st_gid),
								'meta-atime'	    		: str(stat_info.st_atime),
								'meta-mtime'	    		: str(stat_info.st_mtime),
								'meta-ctime'	    		: str(stat_info.st_ctime),
								'meta-mimetype'	    		: mime_type,
								'meta-encode'	    		: encode_type,
								'meta-extension'		: extension_name
								}})
			loop_again = False

		except botocore.exceptions.ClientError as e:
			error_code = int(e.response['Error']['Code'])
			print(" {} Error uploading file {} to bucket {}".
				format(error_code, full_file_name, bucketname))
			sys.exit(1)
		except (botocore.vendored.requests.exceptions.ConnectionError,
				botocore.vendored.requests.exceptions.ReadTimeout,
				botocore.exceptions.EndpointConnectionError) as f:
				print("{} Timeout uploading - Trying again".format(self.name))
				backoff = 300 - ((2 * max_retries) * 10)
				time.sleep(backoff)
				max_retries -= 1
				if (max_retries < 0):
					error_code = int(f.response['Error']['Code'])
					print("{} Error Giving Up on upload {}".format(error_code, full_file_name))
					sys.exit(1)
		except IOError as e:
			print("I/O error {0}:{1}".format(e.errno, e.strerror))



#################################################################################################
# Upload - Calling function to get mimeinfo  
#################################################################################################
def Upload(bucketname, dir_name, file_name, transfer):

	if len(file_name) == 0:
		full_file_name = "{0}/".format(dir_name)
		directory = True
	else:
		full_file_name = "{0}/{1}".format(dir_name, file_name)
		directory = False

	try:

		mime_type, encode_type, extension_name = Fileinfo(full_file_name)

		stat_info = os.stat(full_file_name)

		Uploadfile(bucketname, directory, full_file_name, transfer, stat_info,
			mime_type, encode_type, extension_name)

	except IOError as e:
		print("I/O error {}:{}".format(e.errorno, e.strerror))

#################################################################################################
# setup_connection - setup the connection to the objecstore
#################################################################################################
def setup_connection(env_variables):

	try:

		session = boto3.session.Session()

		# Setup a resource for checking permissions
		s3 = session.resource('s3',
				use_ssl=False,
				endpoint_url=env_variables['endpoint'],
				aws_access_key_id=env_variables['aws_access_key_id'],
				aws_secret_access_key=env_variables['aws_secret_access_key']
				)

		# Setup a transfer agent for moving data
		client = s3.meta.client

		config = TransferConfig(
	                multipart_threshold= 1024 * 1024 * 15, #15MB file size
	                max_concurrency=10,
	                )

		transfer = S3Transfer(client, config)

	except botocore.exceptions.ClientError as e:
		error_code = int(e.response['Error']['Code'])
		print("Errorcode {} Connecting to {} : with access key {} : secret key {}"
			.format(error_code, env_variables['endpoint'],
							env_variables['aws_access_key_id'],
							env_variables['aws_secret_access_key']))
		sys.exit(1)

	## check to see if bucket exists and have permissions 

	try: 
		client.head_bucket(Bucket=env_variables['bucketname'])

	except botocore.exceptions.ClientError as e: 
		error_code = int(e.response['Error']['Code'])
		print("Invalid bucket for backup {}".format(env_variables['bucketname']))
		print("Please check access key has permission or bucket exists")
		sys.exit(1)


	return s3, client, transfer

#################################################################################################
# getArgs - get the configuration and environmental variables. 
#################################################################################################
def getArgs():

	parser = argparse.ArgumentParser()

	env_variables = {}

	script = os.path.basename(sys.argv[0])

	parser.add_argument("-n", "--num-threads", type=int, dest="num_threads", default=1)
	parser.add_argument("-p", "--profilename", dest="profilename", required=False)
	parser.add_argument("-b", "--bucketname", dest="bucketname", required=True)
	parser.add_argument("directory", metavar='directory', help="Directory to backup to an S3 endpoint")

	args = parser.parse_args()


	if (args.bucketname is None):
		print("Error: No bucket provided", usage, file=sys.stderr, sep=' ')
		parser.print_help()
		sys.exit(1)
	else:
		env_variables['bucketname'] = args.bucketname

	if (args.profilename is None):
		env_variables['profilename'] = "default"
	else: 
		env_variables['profilename'] = args.profilename

	home_dir = os.getenv('HOME')
	config = configparser.ConfigParser()
	config.read([str(home_dir + "/.aws/igauth")])

	if os.path.isfile(str(home_dir + "/.aws/igauth")) is False: 
		print("Config file is not found {}".format(str(home_dir + "/.aws/igauth")))
		sys.exit(1)

	env_variables['aws_access_key_id'] = config.get(env_variables['profilename'],'aws_access_key_id')
	env_variables['aws_secret_access_key'] = config.get(env_variables['profilename'], 'aws_secret_access_key')
	env_variables['endpoint'] = config.get(env_variables['profilename'], 'endpoint')
	env_variables['directory_path'] = args.directory
	env_variables['num_threads'] = args.num_threads

	if os.path.isdir(env_variables['directory_path']) is False:
		print("Invalid Directory {0}".format(env_variables['directory_path']))
		sys.exit(1)

	print("getArgs: Directory Path {}: num_threads {}".
			format(env_variables['directory_path'],env_variables['num_threads']))

	return env_variables
#################################################################################################
# main - Function to start the upload process 
#################################################################################################
if __name__ == '__main__':

	env_variables = {}
	env_variables = getArgs()

	print("Main directory_path : {}".format(env_variables['directory_path']))

	try:
		file = open('/tmp/zerofile.0', 'w+')
		file.close()
	except:
		print('Could not open /tmp/zerofile.0')
		sys.exit(1)


	#file_queue = Queue()
	file_queue = JoinableQueue()
	print("Directory {}: num_threads {}: endpoint {}: access_key {}: secret_access_key {}"
			.format(env_variables['directory_path'],
			env_variables['num_threads'],
			env_variables['endpoint'],
			env_variables['aws_access_key_id'],
			env_variables['aws_secret_access_key']))

	file_processes = [ mUpload(file_queue, env_variables)
                    	for i in range(env_variables['num_threads']) ]

	for w in file_processes:
		w.start()

	for dirName, subdirList, fileList in os.walk(env_variables['directory_path'], topdown=True):
		file_queue.put((dirName, ""))
		for fname in fileList:
			file_queue.put((dirName, fname))

	for i in range(env_variables['num_threads']):
		file_queue.put(None)


	print("Waiting for threads to stop")
	for w in file_processes:
		w.join()
	print("Threads stopped")
