import zipfile
import os
import logging
from os.path import join, relpath


def zip_dir(root, zipfilename, **params):
    zipf = zipfile.ZipFile (zipfilename, 'w', zipfile.ZIP_DEFLATED)

    if 'excludes' in params:
        def should_exclude (directory, filename):
            if file == params['destination_file']: return True
            for exclude in params['excludes']:
                if exclude in directory or exclude in filename: return True
            return False
    else:
        def should_exclude (directory, filename):
            return filename == params['destination_file']

    for directory, subdirs, files in os.walk (root):
        for file in files:
            if should_exclude (directory, file): continue
            arcname = relpath (join (directory, file), root)
            zipf.write (join (directory, file), arcname=arcname)

    zipf.close ()
