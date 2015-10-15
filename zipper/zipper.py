import zipfile
import os
from os.path import join, relpath


def zip_dir(dir_path, zip_file_path, zip_file):
    zipf = zipfile.ZipFile(join(zip_file_path, zip_file), 'w', zipfile.ZIP_DEFLATED)

    for root, dirs, files in os.walk(dir_path):
        for file in files:
            if not file == zip_file:
                if len(dirs) > 0:
                    arcname = relpath(join(root, file), root)
                else:
                    arcname = relpath(join(root, file), join(root, '..'))
                zipf.write(join(root, file), arcname=arcname)
    zipf.close()
