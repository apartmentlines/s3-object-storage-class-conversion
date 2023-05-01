#!/usr/bin/env python

import argparse
import sqlite3
import os
import sys
import traceback
import time
import pprint
pp = pprint.PrettyPrinter(indent=4)

DEFAULT_STORAGE_CLASS = "GLACIER_IR"
DEFAULT_TABLE_NAME = "s3_objects"
DEFAULT_ERROR_LOG = "/tmp/update-storage-class-error.log"
DB_NAME = 's3_objects_list.db'
S3CFG_PATH = os.path.expanduser('~/.s3cfg')


class S3StorageChanger:

    def __init__(self, table, s3_folder_path, storage_class, error_log, gather, update, sleep):
        self.table = table
        self.s3_folder_path = s3_folder_path
        self.storage_class = storage_class
        self.error_log = error_log
        self.gather = gather
        self.update = update
        self.sleep = sleep
        self.s3 = S3(Config(S3CFG_PATH))
        self.conn = sqlite3.connect(DB_NAME.encode('utf-8'))
        self.cur = self.conn.cursor()

    def create_table(self):
        self.conn.execute('''CREATE TABLE IF NOT EXISTS %s
                            (path TEXT PRIMARY KEY)''' % self.table)
        self.conn.commit()

    def insert_s3_object(self, path):
        print("Adding object to cache: %s" % path)
        self.conn.execute("INSERT INTO %s (path) VALUES (?)" % self.table, (path,))
        self.conn.commit()

    def delete_s3_object(self, path):
        print("Deleting object from cache: %s" % path)
        self.conn.execute("DELETE FROM %s WHERE path=?" % self.table, (path,))
        self.conn.commit()

    def get_s3_objects(self):
        return self.cur.execute("SELECT * FROM %s" % self.table)

    def get_top_level_directories(self):
        print("Getting top level dirs in bucket: %s" % self.s3_folder_path)
        response = self.s3.bucket_list(self.s3_folder_path, recursive=False)
        return [item['Prefix'] for item in response['common_prefixes']]

    def get_objects_in_directory(self, directory):
        print("Getting objects in dir: %s" % directory)
        time.sleep(self.sleep)
        response = self.s3.bucket_list(self.s3_folder_path, prefix=directory, recursive=True)
        return [item['Key'] for item in response['list'] if item['StorageClass'] != self.storage_class]

    def change_storage_class(self):
        self.create_table()

        if self.gather:
            top_directories = self.get_top_level_directories()
            for directory in top_directories:
                objects = self.get_objects_in_directory(directory)
                for s3_object in objects:
                    self.insert_s3_object(s3_object)

        if self.update:
            for row in self.get_s3_objects():
                s3_object_path = row[0]
                print("Changing storage class of object: %s" % s3_object_path)
                time.sleep(self.sleep)
                full_s3_path = S3Uri("s3://%s/%s" %(self.s3_folder_path, s3_object_path))
                try:
                    self.s3.object_modify(full_s3_path, full_s3_path, extra_headers={'x-amz-storage-class': self.storage_class})
                    print("Successfully changed storage class of %s to %s" % (s3_object_path, self.storage_class))
                    self.delete_s3_object(s3_object_path)
                except Exception as e:
                    message = "Error changing storage class of %s to %s: %s" % (s3_object_path, self.storage_class, e)
                    print(message)
                    with open(self.error_log, 'a') as f:
                        f.write(message)

        self.conn.close()


def main():
    parser = argparse.ArgumentParser(description="A script to change the storage class of objects in an S3 bucket.")
    parser.add_argument("s3_folder_path", help="S3 folder path for the objects to be updated")
    parser.add_argument("-t", "--table", default=DEFAULT_TABLE_NAME,
                        help="Table in the SQLite database to use. Default is %s" % DEFAULT_TABLE_NAME)
    parser.add_argument("-s", "--storage-class", default=DEFAULT_STORAGE_CLASS,
                        help="Desired storage class for the S3 objects. Default is %s" % DEFAULT_STORAGE_CLASS)
    parser.add_argument("-e", "--error-log", default=DEFAULT_ERROR_LOG,
                        help="File to log errors when updating storage class. Default is %s" % DEFAULT_ERROR_LOG)
    parser.add_argument("--gather", action="store_true",
                        help="Gather objects in s3_folder_path to store in the SQLite database")
    parser.add_argument("--update", action="store_true",
                        help="Change storage class for objects already stored in the SQLite database")
    parser.add_argument("--sleep", type=float, default=0,
                        help="Time in seconds to sleep between processing S3 objects")
    args = parser.parse_args()

    if args.s3_folder_path is None:
        parser.error("S3 folder path is required")

    s3_storage_changer = S3StorageChanger(args.table, args.s3_folder_path, args.storage_class, args.error_log, args.gather, args.update, args.sleep)
    s3_storage_changer.change_storage_class()


def report_exception(e):
    sys.stderr.write("""
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    An unexpected error has occurred.
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

""")
    tb = traceback.format_exc(sys.exc_info())
    e_class = str(e.__class__)
    e_class = e_class[e_class.rfind(".")+1 : -2]
    sys.stderr.write(u"Problem: %s: %s\n" % (e_class, e))
    try:
      sys.stderr.write("S3cmd version:   %s\n" % PkgInfo.version)
    except NameError:
      sys.stderr.write("S3cmd:   unknown version. Module import problem?\n")
    sys.stderr.write("\n")
    sys.stderr.write(unicode(tb, errors="replace"))

    if type(e) == ImportError:
      sys.stderr.write("\n")
      sys.stderr.write("Your sys.path contains these entries:\n")
      for path in sys.path:
        sys.stderr.write(u"\t%s\n" % path)
      sys.stderr.write("Now the question is where have the s3cmd modules been installed?\n")

    sys.stderr.write("""
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    An unexpected error has occurred.
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
""")

if __name__ == '__main__':

  # TODO: Hack for now to include path of s3cmd modules.
  sys.path.append("/usr/local/Cellar/s3cmd/1.6.1/libexec/lib/python2.7/site-packages")
  sys.path.append("/usr/share/s3cmd")

  try:
    ## Our modules
    ## Keep them in try/except block to
    ## detect any syntax errors in there
    from S3.Exceptions import *
    from S3 import PkgInfo
    from S3.S3 import S3
    from S3.Config import Config
    from S3.S3Uri import S3Uri
    from S3.Utils import *

    main()
    sys.exit(0)

  except ImportError as e:
    report_exception(e)
    sys.exit(1)

  except ParameterError as e:
    error(u"Parameter problem: %s" % e)
    sys.exit(1)

  except SystemExit as e:
    sys.exit(e.code)

  except KeyboardInterrupt:
    sys.stderr.write("See ya!\n")
    sys.exit(1)

  except Exception as e:
    report_exception(e)
    sys.exit(1)
