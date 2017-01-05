#-------------------------------------------------------------------------------------------
#-----SWAMI NIKHIL NAGENDRA-----UTA ID : 1000915606-----------------------------------------
#----- 6331 --- Cloud Computing----AISSGNMENT NO 2------------------------------------------
#-------------------------------------------------------------------------------------------
import os
import sys
import glob
import subprocess
import contextlib
import functools
import multiprocessing
from multiprocessing.pool import IMapIterator
from optparse import OptionParser
import rfc822
from time import time 
import boto #import for boto
from boto.s3.connection import S3Connection
#-------------------------------------------------------------------------------------------
#----------------------SINGLE TRANSFER FOR FILE LESS THAN 10 MB-----------------------------
def _standard_transfer(bucket, s3_key_name, transfer_file, use_rr):
    print " Upload with standard transfer, not multipart",
    new_s3_item = bucket.new_key(s3_key_name)
    new_s3_item.set_contents_from_filename(transfer_file, reduced_redundancy=use_rr)
    print
#-------------------------------------------------------------------------------------------
def map_wrap(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        return apply(f, *args, **kwargs)
    return wrapper
#-------------------------------------------------------------------------------------------
def mp_from_ids(mp_id, mp_keyname, mp_bucketname):
    """Get the multipart upload from the bucket and multipart IDs.

    This allows us to reconstitute a connection to the upload
    from within multiprocessing functions.
    """
    conn = boto.connect_s3()
    bucket = conn.lookup(mp_bucketname)
    mp = boto.s3.multipart.MultiPartUpload(bucket)
    mp.key_name = mp_keyname
    mp.id = mp_id
    return mp
#----------------------------UPLOADING PART OF FILE-----------------------------------------
@map_wrap
def transfer_part(mp_id, mp_keyname, mp_bucketname, i, part):
    """Transfer a part of a multipart upload. Designed to be run in parallel.
    """
    mp = mp_from_ids(mp_id, mp_keyname, mp_bucketname)
    print " Transferring", i, part
    with open(part) as t_handle:
        mp.upload_part_from_file(t_handle, i+1)
    os.remove(part)
#-------------------------------FILE SPLITTING FUNCTION-------------------------------------
#-----CODE MAY BE DERIVED FROM -------------------------------------------------------------
#---http://bdurblg.blogspot.com/2011/06/python-split-any-file-binary-to.html----------------
#-------define the function to split the file into smaller chunks---------------------------
def splitFile(inputFile,chunkSize):

#read the contents of the file
    f = open(inputFile, 'rb')
    data = f.read() # read the entire content of the file
    f.close()

# get the length of data, ie size of the input file in bytes
    bytes = len(data)

    #calculate the number of chunks to be created
    noOfChunks= bytes/chunkSize
    if(bytes%chunkSize):
        noOfChunks+=1

    #create a info.txt file for writing metadata
    f = open('info.txt', 'w')
    f.write(inputFile+','+'chunk,'+str(noOfChunks)+','+str(chunkSize))
    f.close()



    chunkNames = []
    for i in range(0, bytes+1, chunkSize):
        fn1 = "chunk%s" % i
        chunkNames.append(fn1)
        f = open(fn1, 'wb')
        f.write(data[i:i+ chunkSize])
        f.close()
    return chunkNames
#-------------------------MULTIPART UPLOAD FUNCTION-----------------------------------------
def _multipart_upload(bucket, s3_key_name, tarball, mb_size, use_rr=True,
                      cores=None):
    
    mp = bucket.initiate_multipart_upload(s3_key_name, reduced_redundancy=use_rr)
    
    chunkNames = splitFile(tarball,6000000)
    j=1
    for i in chunkNames:
        fp = open(i, 'rb')
        mp.upload_part_from_file(fp, j)
        j=j+1
        fp.close()
    mp.complete_upload()
#-------------------------------------------------------------------------------------------
@contextlib.contextmanager
def multimap(cores=None):
    
    if cores is None:
        cores = max(multiprocessing.cpu_count() - 1, 1)
    def wrapper(func):
        def wrap(self, timeout=None):
            return func(self, timeout=timeout if timeout is not None else 1e100)
        return wrap
    IMapIterator.next = wrapper(IMapIterator.next)
    pool = multiprocessing.Pool(cores)
    yield pool.imap
    pool.terminate()
#-----------------------------MAIN PROGRAM---------------------------------------------------
transfer_file = "performance.doc"
bucket_name = "hcg5kujt"
use_rr=True
s3_key_name = os.path.basename(transfer_file)
conn = S3Connection("AKIAJWPALCSUZ45EIJZA","l/cVJG2CmvpAXKuBmOtSfudETgIMnZYj29dYXEGK")
bucket = conn.lookup(bucket_name)
cores=None
if bucket is None:
    bucket = conn.create_bucket(bucket_name)
mb_size = os.path.getsize(transfer_file) / 1e6
if mb_size < 10:
    t = time()
    _standard_transfer(bucket, s3_key_name, transfer_file, use_rr)
    e = time()
    print "time used", e-t
else:
    _multipart_upload(bucket, s3_key_name, transfer_file, mb_size, use_rr,
                      cores)
    t = time()
    _standard_transfer(bucket, s3_key_name, transfer_file, use_rr)
    e = time()
    print "time used", e-t
#-------------------------------------------------------------------------------------------
