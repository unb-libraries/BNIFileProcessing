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
from sqlalchemy import event, DDL

import datetime
import fnmatch
import os as os
import pyprind
import subprocess
import shutil
import sys

Base = declarative_base()


class BNIImage(Base):
    __tablename__ = 'bni_image'
    uuid = Column(Integer(),
                  Sequence('article_aid_seq', start=512083, increment=1),
                  primary_key=True)
    name = Column(String(512), nullable=False)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)
event.listen(BNIImage.__table__, "after_create", DDL("INSERT INTO bni_image (uuid, name) VALUES (512082,'START')"))


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
        if len(self.files_to_process) < 400:
            print("\nERROR: Number of TIF files in source tree is suspiciously low. (--source)")
            sys.exit(2)
        if len(self.files_to_process) > 2000:
            print("\nERROR: Number of TIF files in source tree is suspiciously high. (--source)")
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
        if not os.path.isfile('bni_images.db'):
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
        self.option_parser.add_option(
            "-t", "--target",
            dest="target_path",
            default='',
            help="The mounted Amazon S3FS target where the files will be stored. This is used only to determine what directory name to place files into, it is not written to.",
        )
        self.option_parser.add_option(
            "-n", "--next",
            dest="next_dir",
            default=False,
            help="Use this option to manually override the next_dir setting with a new dirname, e.g. 00049.",
        )
        (options, args) = self.option_parser.parse_args()
        self.options = options
        self.check_options()

    def process(self):
        progress_bar = pyprind.ProgPercent(len(self.files_to_process))
        os.makedirs(self.options.bni_path + "/" + self.next_dir)
        os.makedirs(self.options.lib_path + "/" + self.next_dir)

        for tif_filename in self.files_to_process:
            relative_tif_path = tif_filename.replace(self.options.source_path, '')
            image_uuid = self.get_image_uuid(relative_tif_path)
            self.process_worker(tif_filename, relative_tif_path, image_uuid, progress_bar)

        self.check_file_count(self.options.bni_path + "/" + self.next_dir, 'tif')
        self.check_file_count(self.options.lib_path + "/" + self.next_dir, 'jpg')

        self.generate_sha1_tree(self.options.bni_path + "/" + self.next_dir, 'tif')
        self.generate_sha1_tree(self.options.lib_path + "/" + self.next_dir, 'jpg')

        self.copy_tree_bni()

    def process_worker(self, tif_filename, relative_tif_path, image_uuid, progress_bar):
        file_stem = os.path.basename(
            tif_filename[0:tif_filename.rindex('.')]
        )
        jpg_filename = os.path.normpath(
            os.path.dirname(tif_filename) + '/' +
            '../Jpgs/' +
            '.'.join((file_stem, 'jpg'))
        )
        self.archive(tif_filename, relative_tif_path, self.options.bni_path, image_uuid)
        self.archive(jpg_filename, relative_tif_path, self.options.lib_path, image_uuid)
        progress_bar.update()

    def set_files_to_process(self):
        """ Populates the files_to_process list with TIF files from the source_path tree. """
        for root, dirnames, filenames in os.walk(self.options.source_path):
            for filename in fnmatch.filter(filenames, '*.tif'):
                self.files_to_process.append(root + '/' + filename)

    def set_next_dir(self):
        """ Examines the bni_path to determine the next path in the numerically sequenced directories. """
        if self.options.next_dir is False:
            file_counter = 1
            while path.exists(self.options.target_path + "/" + str(file_counter).zfill(6)):
                file_counter += 1
            self.next_dir = str(file_counter).zfill(6)
        else:
            self.next_dir = self.options.next_dir

    def get_image_uuid(self, relative_tif_path):
        new_image = BNIImage(name=relative_tif_path)
        self.db_session.add(new_image)
        self.db_session.flush()
        self.db_session.commit()
        return new_image.uuid

    def archive(self, source_filename, relative_tif_path, target_path, uuid):
        full_target_path = target_path + '/' + str(self.next_dir) + os.path.normpath(os.path.dirname(relative_tif_path) + '/../')
        if not os.path.exists(full_target_path):
            os.makedirs(full_target_path)
        new_filename = str(uuid) + '__' + os.path.basename(source_filename)
        new_filepath = full_target_path + '/' + new_filename

        move_call = [
            '/bin/mv',
            source_filename,
            new_filepath
        ]
        subprocess.call(move_call)

    def generate_sha1_tree(self, path, file_type):
        sha1sum_call = 'find . -type f -name "*.' + file_type + '" -print0 | xargs -0 sha1sum > ' + self.next_dir + '.sha1'
        subprocess.call(sha1sum_call, cwd=path, shell=True)

    def copy_tree_bni(self):
        bni_tree_copy_call = 'aws s3 sync ' + self.next_dir + ' "s3://bni-digital-archives-scans/' + self.next_dir + '"'
        subprocess.call(bni_tree_copy_call, cwd=self.options.bni_path, shell=True)

    def check_file_count(self, path, extension):
        dir_files = self.get_num_files_in_tree(path, extension)
        if not len(self.files_to_process) == self.get_num_files_in_tree(path, extension):
            print(
                "\nERROR: The number [" + str(len(dir_files)) + '] of generated ' + extension + ' files in ' + path +
                ' does not match the source [' + str(len(self.files_to_process)) + '] in ' + self.options.source_path + ' !')
            sys.exit(2)

    def get_num_files_in_tree(self, path, extension):
        dir_files = []
        for root, dirnames, filenames in os.walk(path):
            for filename in fnmatch.filter(filenames, '*.' + extension):
                dir_files.append(root + '/' + filename)
        return len(dir_files)

    def delete_source_dir(self):
        if self.get_num_files_in_tree(self.options.source_path, 'tif') == 0:
            print "Removing source dir: " + self.options.source_path
            if not self.options.source_path == '/' and not self.options.source_path == '':
                shutil.rmtree(self.options.source_path)
        else:
            print "Cowardly refusing to remove non-empty source dir: " + self.options.source_path
