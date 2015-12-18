"""BNIImageProcessor
Manipulates and transfers scanned newspaper images in a defined workflow.
This suite is really only useful in our specific situation. If you are looking at
this project from afar, you probably do not want to use it.
"""
from optparse import OptionParser
from os import path
from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import Sequence
from sqlalchemy.sql import exists
import datetime
import fnmatch
import os as os
import sys


Base = declarative_base()


class BNIImage(Base):
    __tablename__ = 'bni_image'
    uuid = Column(Integer(),
                  Sequence('article_aid_seq', start=512083, increment=1),
                  primary_key=True)
    name = Column(String(512), nullable=False)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)


class BNIImageProcessor(object):
    def __init__(self):
        self.option_parser = None
        self.options = None
        self.db_session = None
        self.next_dir = None
        self.files_to_process = []

        self.init_options()
        self.check_target()
        self.set_next_dir()
        self.set_files_to_process()
        self.check_source_files()
        self.init_database()
        self.check_already_processed_files()

    def check_already_processed_files(self):
        """ Checks to see if any files in files_to_process have already been assigned an UUID. """
        already_processed_files = self.get_already_processed_files()
        if len(already_processed_files) > 0:
            print("\nERROR: Some files in this set have already been assigned a UUID")
            print already_processed_files
            sys.exit(2)

    def check_options(self):
        """ Checks to ensure that all necessary options were passed and valid. """
        if self.options.source_path is None or not path.exists(self.options.source_path):
            self.option_parser.print_help()
            print("\nERROR: Cannot read source path! (--source)")
            sys.exit(2)
        if self.options.bni_path is None or not os.access(self.options.bni_path, os.W_OK):
            self.option_parser.print_help()
            print("\nERROR: Cannot write to BNI path! (--bni)")
            sys.exit(2)
        if self.options.lib_path is None or not os.access(self.options.lib_path, os.W_OK):
            self.option_parser.print_help()
            print("\nERROR: Cannot write to LIB path! (-l)")
            sys.exit(2)
        if self.options.target_path is None or not path.exists(self.options.target_path):
            self.option_parser.print_help()
            print("\nERROR: Cannot read target path! (--target)")
            sys.exit(2)

    def check_source_files(self):
        """ Checks the source files for simple problems. """
        if len(self.files_to_process) < 800:
            print("\nERROR: Number of TIF files in source tree is suspiciously low. (--source)")
            sys.exit(2)
        if len(self.get_unmatched_tifs()) > 0:
            print("\nERROR: Some TIF files appear to not have corresponding JPG files. (--source)")
            print self.get_unmatched_tifs()
            sys.exit(2)

    def check_target(self):
        """ Checks the target files for simple problems. """
        if not path.isdir(self.options.target_path.rstrip("/") + '/000007') or not path.isdir(self.options.target_path.rstrip("/") + '/000028'):
            print("\nERROR: Target does not look like I expected! (--target)")
            sys.exit(2)

    def get_already_processed_files(self):
        """ Queries the database to return any items in files_to_process that have already been assigned an UUID. """
        files_already_processed = []
        for tif_filename in self.files_to_process:
            relative_tif_path = tif_filename.replace(self.options.source_path, '')
            if self.db_session.query(exists().where(BNIImage.name == relative_tif_path)).scalar():
                files_already_processed.append(relative_tif_path)
        return files_already_processed

    def get_unmatched_tifs(self):
        """ Checks if any TIF files in files_to_process do not have corresponding JPG files where expected. """
        files_without_jpg = []
        for tif_filename in self.files_to_process:
            jpg_file_path = os.path.dirname(tif_filename) + '/../Jpgs/' + '.'.join((os.path.splitext(os.path.basename(tif_filename))[0], 'jpg'))
            if not os.path.isfile(jpg_file_path):
                files_without_jpg.append(jpg_file_path)
        return files_without_jpg

    def init_database(self):
        """ Creates the database connection and sets the session. """
        engine = create_engine('sqlite:///bni_images.db')
        Base.metadata.create_all(engine)
        DBSession = sessionmaker(bind=engine)
        self.db_session = DBSession()

    def init_options(self):
        """ Defines and initialize the options passed at the CLI. """
        self.option_parser = OptionParser()
        self.option_parser.add_option(
            "-s", "--source",
            dest="source_path",
            default='',
            help="The source path to copy the files from. No trailing slash.",
        )
        self.option_parser.add_option(
            "-t", "--target",
            dest="target_path",
            default='',
            help="The mounted Amazon S3FS target where the files will be stored. This is used only to determine what directory name to place files into, it is not written to.",
        )
        self.option_parser.add_option(
            "-b", "--bni",
            dest="bni_path",
            default='',
            help="The path that BNI files should be copied to.",
        )
        self.option_parser.add_option(
            "-l", "--lib",
            dest="lib_path",
            default='',
            help="The path that LIB files should be copied to.",
        )
        (options, args) = self.option_parser.parse_args()
        self.options = options
        self.check_options()

    def set_files_to_process(self):
        """ Populates the files_to_process list with TIF files from the source_path tree. """
        for root, dirnames, filenames in os.walk(self.options.source_path):
            for filename in fnmatch.filter(filenames, '*.tif'):
                self.files_to_process.append(root + '/' + filename)

    def set_next_dir(self):
        """ Examines the target_path to determine the next path in the numerically sequenced directories. """
        file_counter = 1
        while path.exists(self.options.target_path + "/" + str(file_counter).zfill(6)):
            file_counter += 1
        self.next_dir = self.options.target_path + str(file_counter).zfill(6)
