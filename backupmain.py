#!/usr/bin/env python3


import argparse
import os 
import sys
import time
import boto3
import botocore
import mimetypes
from multiprocessing import Process, JoinableQueue, Queue

# https://pymotw.com/2/multiprocessing/basics.html

# class for managing the threads that are spawned,  filenames are passed into the 
# thread through the a joinable queue,  the filesize that is uploaded is returned
# via another queue that ends up caculating total system throughput.
class Fileuploader(Process):

	def __init__(self, file_queue, result_queue, endpoint, 
			bucketname, access_key, secret_access_key):
		Process.__init__(self)
		self.file_queue = file_queue
		self.result_queue = result_queue
		self.endpoint = endpoint
		self.bucketname = bucketname
		self.access_key = access_key
		self.secret_access_key = secret_access_key

	def run(self):
	
		total_file_count = 0
		total_file_size = 0
		# set the threadname for use in debugging
		proc_name = self.name
		
		#Call the Connect function to setup the s3 connection, each thread makes its own connection
		transfer, client, config = Connect(self.endpoint, self.access_key, self.secret_access_key)
	
		# Listen and loop on the queue until it hits a None and then gracefully shutdown	
		while True:
			# Get the next item in the queue
			next_task = self.file_queue.get()

			print("{}: Queue String: {}".format(proc_name,next_task))
	
			# if the non market is hit, gracefully shutdown the thread	
			if next_task is None:
				print("Exiting: {}".format(proc_name))
				self.file_queue.task_done()
				break

			print(" dir_name {}, file_name {}".format(next_task[0], next_task[1]))
			
			# upload the object into S3. 			
			file_size = Upload(self.bucketname, next_task[0], next_task[1], transfer)
			total_file_size += file_size 
			total_file_count += 1

			# put the size of the object into the results queue for processing later

		print("{}: Queue String: {}".format(proc_name,next_task))
		print("Total Files {} : Total File Size {}".format(total_file_count, total_file_size))
		self.result_queue.put((total_file_count, total_file_size))
		return	

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

def Uploadfile(bucketname, directory, full_file_name, transfer, stat_info, mime_type, encode_type, extension_name):
	
	full_file_name_woslash = full_file_name[1:]
	
	if directory is True: 
		full_file_name = "/tmp/zerofile.0"

	try: 
		transfer.upload_file(full_file_name, 
					bucketname, 
					full_file_name_woslash,
					extra_args={"Metadata":{'backup-date'		: time.ctime(),
								'meta-mode'	: str(stat_info.st_mode),
								'meta-uid'	: str(stat_info.st_uid),
								'meta-gid'	: str(stat_info.st_gid),
								'meta-atime'	: str(stat_info.st_atime),
								'meta-mtime'	: str(stat_info.st_mtime),
								'meta-ctime'	: str(stat_info.st_ctime),
								'meta-mimetype'	: mime_type,
								'meta-encode'	: encode_type,
								'meta-extension'	: extension_name
								}}
					)
		
	except botocore.exceptions.ClientError as e:
		error_code = e.response['Error']['Code']
		print("Upload File Error : ErrorCode >{}< Unexpected error {} {}".format(error_code, e,e.response['Error']['Code']))
		print("Upload File Error : Filename {} : Full Name of File {}".format(full_file_name, full_file_name_woslash))
		sys.exit(1)

	except IOError as f:
		print("I/O error {0}:{1}".format(f.errno, f.strerror))

	
	
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
		file_size = stat_info.st_size

		Uploadfile(bucketname, directory, full_file_name, transfer, stat_info, mime_type, encode_type, extension_name)

	except IOError as e:
		print("I/O error {}:{}".format(e.errno, e.strerror))

	return file_size


def Connect(endpoint, access_key, secret_access_key): 

# Setup the connection for the thread to s3

	try: 
		client = boto3.client (
					's3',
					use_ssl=False,
					endpoint_url=endpoint,
					aws_access_key_id=access_key,
					aws_secret_access_key=secret_access_key
					)

		config = boto3.s3.transfer.TransferConfig(
				multipart_threshold=20 * 1024 * 1024 * 1024, 
				max_concurrency = 10,
			)

		transfer = boto3.s3.transfer.S3Transfer(client, config) 

	except botocore.exceptions.ClientError as e:
		print("Unexpected error {}".format(e))
		print("Cannot connect to S3 endpoint")
		sys.exit(1)

	except IOError as f:
		print("I/O error {}:{}".format(f.errorno, f.strerror))

	return transfer, client, config
		
	
def getArgs():

	parser = argparse.ArgumentParser()

	script = os.path.basename(sys.argv[0])
	usage = 'Usage: ' + script + '-n NUM_THREADS directory'

	parser.add_argument("directory", metavar='directory', help="Directory to backup to an S3 endpoint")
	parser.add_argument("-n", "--num-threads", type=int, dest="num_threads", default=1)
	parser.add_argument("-v", "--verbose", dest="verbosity", default=False)
	parser.add_argument("-e", "--s3-endpoint", dest="endpoint", required=True)
	parser.add_argument("-b", "--bucketname", dest="bucketname", required=True)
	parser.add_argument("-a", "--access-key",  dest="access_key")
	parser.add_argument("-s", "--secret-access-key", dest="secret_access_key")

	args = parser.parse_args()
	
	if (args.endpoint is None): 
		print("Error: No endpoint provided", usage, file=sys.stderr, sep='')
		parser.print_help()
		sys.exit(1)
	else: 
		endpoint = args.endpoint

	if (args.bucketname is None):
		print("Error: No bucket provided", usage, file=sys.stderr, sep=' ')
		parser.print_help()
		sys.exit(1)
	else:
		bucketname = args.bucketname

	if (args.access_key is None):
		access_key = "ACCESSKEY"
	else:
		access_key = args.access_key
		
	if (args.secret_access_key is None):
		secret_access_key = "SECRETACCESSKEY"
	else:
		secret_access_key = args.secret_access_key


	directory_path = args.directory
	num_threads = args.num_threads

	if os.path.isdir(directory_path) is False:
		print("Invalid Directory {0}".format(directory_path))
		sys.exit(1)

	print("getArgs: Directory Path {}: num_threads {}".format(directory_path, num_threads))

	return directory_path, num_threads, endpoint, bucketname, access_key, secret_access_key

def processRun(inputQueue):

	while inputQueue.empty() is False:
		input_string = inputQueue.get()
		print("Queue String: {}".format(input_string))


if __name__ == '__main__':


	directory_path, num_threads, endpoint, bucketname, access_key, secret_access_key = getArgs()

	print("Main directory_path : {}".format(directory_path))

	try: 
		file = open('/tmp/zerofile.0', 'w+')
		file.close()
	except: 
		print('Could not open /tmp/zerofile.0')
		sys.exit(1)

	file_queue = JoinableQueue()
	result_queue = Queue()

	print("Directory {}: num_threads {}: endpoint {}: access_key {}: secret_access_key {}"
			.format(directory_path, num_threads, endpoint, access_key, secret_access_key))

	file_processes = [ Fileuploader(file_queue, result_queue, endpoint, bucketname, access_key, secret_access_key)
                              for i in range(num_threads) ]			   

	for w in file_processes:
		w.start()

	start_time = time.time()
	for dirName, subdirList, fileList in os.walk(directory_path, topdown=True):
		file_queue.put((dirName, ""))
		for fname in fileList:
			file_queue.put((dirName, fname))

	for i in range(num_threads):
		file_queue.put(None)



	print("Waiting for threads to stop")
	for w in file_processes:
		w.join()
	
	#file_queue.join()

	end_time = time.time()


	#print("Entries in return queue {}".format(queue_size))

	total_bytes = 0 	
	total_files = 0 
	count = 0
	size = 0

	
	#while num_threads: 
	for i in range(num_threads):
		count = result_queue.get()
		print("values {}".format(count))
		total_files += count[0]
		total_bytes += count[1]
	#	result_queue.task_done()


	print("Total bytes transferrer {} : total_files{}".format(total_bytes, total_files))
	total_bytes = (total_bytes / (1024 * 1024))
	print("Throughput: {} MB/s".format(total_bytes / (end_time - start_time)))		
	print("Threads stopped")
		




