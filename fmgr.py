import os
import glob
import collections
import shutil
from pathlib import Path
import hashlib
import zipfile
from datetime import datetime as dt
from optparse import OptionParser
import traceback
from tqdm import tqdm
import PIL
from PIL import Image
import pdb

KB = 2**10
MB = 2**20
GB = 2**30

UNITS_DISPLAY = [(2**10, "B"), (2**20, "KB"), (2**30, "MB")]
def fmtnum(size):
    for limit, unit in UNITS_DISPLAY:
        if size < limit:
            return str(round(size / (limit / 1024), 2)) + unit
    return str(round(size / 2**30, 2)) + "GB"


class FileInfo:
    """
    For holding file information
    """
    def __init__(self, path, size):
        self.path = path
        self.size = size

    def __str__(self):
        return "File: {}, size: {}, percent of same extension: {}%, percent of all extensions {}%"\
            .format(self.path, fmtnum(self.size), self.percent, self.percent_all)


class ExtInfo:
    """
    For holding information for all files with a specific extension
    """
    def __init__(self, ex, nfiles, size, comp_size):
        self.ex = ex
        self.nfiles = nfiles
        self.size = size
        self.comp_size = comp_size
        self.diff = size - comp_size
        self.diff_percent = 100*self.diff/size

    def __str__(self):
        return "Extension {}: n_files={}".format(self.ex, self.nfiles)


class FileMgmt:
    """
    This class implements specialized files management for backup, indexing and smart compression.
    * compress_report               shows a report for zip compression per files extensions
    * jpg_quality_reduce_report     shows a report for possible data reduction using jpeg quality loss;
                                    especially excellent reduction for documents pictures
    """
    def __init__(self, path):
       self.path = path
       self.extdicts = {}

    def list_files(self, dir_path):
        """ return list of files in dir_path no recursive"""
        fullpath = os.path.join(dir_path, "*")
        l = glob.glob(fullpath)
        return l

    @staticmethod
    def list_files_recursive(dir_path):
        dir_path = Path(dir_path)
        return dir_path.glob('**/*')

    def show_ext_count(self, dir_path):
        iter_files = self.list_files(dir_path)
        extdict = collections.defaultdict(int)
        for fullpath in iter_files:
            _, file_extension = os.path.splitext(fullpath)
            extdict[file_extension[1:]] += 1
        for filext, cnt in extdict.items():
            print(cnt, filext)

    def filestat(self, dir_path):
        iter_files = self.list_files(dir_path)
        d = {}
        for filename in iter_files:
            fname, _ = os.path.splitext(filename)
            fs = os.stat(filename)
            modtime = dt.fromtimestamp(fs.st_mtime)
            d[filename] = str(modtime)
        return d

    DATE_PART_IX = 7
    def organizefiles(self, source, dest):
        d = self.filestat(source)
        for filepath, fdate in d.items():
            foldername = fdate[:self.DATE_PART_IX].replace("-", "_")
            destpath = os.path.join(dest, foldername)
            destfile = os.path.join(destpath, os.path.basename(filepath))
            if not os.path.exists(destpath):
                os.mkdir(destpath)
            print("copying {} --> {}".format(filepath, destfile))
            shutil.copyfile(filepath, destfile)

    def show_treesize(self, dir_path):
        folder_sum = 0
        f_list = []
        iter_files = self.list_files_recursive(dir_path)
        for filename in iter_files:
            if filename.is_file():
                fsize = filename.stat().st_size
                f_obj = FileInfo(filename, fsize)
                folder_sum += fsize
                f_list.append((filename, f_obj))
        f_list.sort(key=lambda tup:tup[1].size, reverse=True)
        for fullpath, f_obj in f_list:
            print(fullpath, fmtnum(f_obj.size), str(round((f_obj.size / folder_sum) * 100, 2)) + "%")

    BLOCKSIZE = 64*KB
    def generate_hash(self, file_path):
        file_hash = hashlib.sha1()
        with open(file_path, 'rb') as f:
            buf = f.read(self.BLOCKSIZE)
            while len(buf) > 0:
                file_hash.update(buf)
                buf = f.read(self.BLOCKSIZE)
        return file_hash.hexdigest()

    def find_duplicates(self, dir_path):
        dict_hash = collections.defaultdict(list)
        file_list = self.list_files_recursive(dir_path)
        for fullpath in file_list:
            if fullpath.is_file():
                f_hash = self.generate_hash(fullpath)
                dict_hash[f_hash].append(fullpath)
        for _, dup_files in dict_hash.items():
            if len(dup_files) > 1:
                print("Duplicated files: {}".format(dup_files))

    def get_ext_map(self, dir_path):
        if dir_path in self.extdicts:
            return self.extdicts[dir_path]
        iter_files = self.list_files_recursive(dir_path)
        ext_dict = collections.defaultdict(list)
        for file_path in iter_files:
            try:
                if file_path.is_file():
                    _, file_ext = os.path.splitext(file_path)
                    curr_size = file_path.stat().st_size
                    fi = FileInfo(file_path, curr_size)
                    ext_dict[file_ext[1:]].append(fi)
            except PermissionError:
                pass # ignoring files with no permission
        self.extdicts[dir_path] = ext_dict
        return self.extdicts[dir_path]

    def extension_stats(self, dir_path, n=5, should_print=False):
        ext_dict = self.get_ext_map(dir_path)
        ext_sum_all = 0
        for _, values, in ext_dict.items():
            ext_sum = sum([f.size for f in values])
            ext_sum_all += ext_sum
            for f in values:
                f.percent = round((f.size / ext_sum) * 100, 2)
        for _, values, in ext_dict.items():
            for f in values:
                f.percent_all = round((f.size / ext_sum_all) * 100, 2)
        for _, v in ext_dict.items():
            v.sort(key=lambda curr_f:curr_f.size, reverse=True)
        if should_print:
            for e, v in ext_dict.items():
                print("Top {} files in extension {}:".format(len(v[:n]), e))
                for value in v[:n]:
                    print(value)

    def compress_file(self, f):
        sample_filepath = 'sample.zip'
        with zipfile.ZipFile(sample_filepath, 'w') as zf:
            zf.write(f, compress_type=zipfile.ZIP_DEFLATED)
        if os.path.exists(sample_filepath):
            os.remove(sample_filepath)
        return zf

    def compress_report(self):
        print("================== COMPRESS REPORT ==================")
        ext_dict = self.get_ext_map(self.path)
        print("Calculating compression ratio...")
        ext_info_list = []
        for ext, files_list in tqdm(ext_dict.items()):
            total_size = 0
            total_size_comp = 0
            for f in files_list:
                zf = self.compress_file(f.path)
                comp_file = zf.namelist()[0]
                total_size += zf.getinfo(comp_file).file_size
                total_size_comp += zf.getinfo(comp_file).compress_size
            ei = ExtInfo(ext, len(files_list), total_size, total_size_comp)
            ext_info_list.append(ei)

        for ei in ext_info_list:
            if ei.diff > (100*MB):
                print("extension {}:  {} files, size {} comp_size {} diff {} ({}%)"
                      .format(ei.ex, ei.nfiles, fmtnum(ei.size), fmtnum(ei.comp_size),
                              fmtnum(ei.diff), round(ei.diff_percent, 2)))
        print("=====================================================")

    def jpg_quality_reduce_report(self, quality):
        print("==================== JPG REPORT ====================")
        sample_file = os.path.join(self.path, "sample.jpg")
        for dir_path in Path(self.path).iterdir():
            if not dir_path.is_dir():
                continue
            em = self.get_ext_map(dir_path)
            total_before = total_after = 0
            jpg_extensions = ["jpg", "jpeg", "JPG"]
            num_files = sum([len(em[x]) for x in jpg_extensions])
            for ex_type in jpg_extensions:
                for jpg_fi in em[ex_type]:
                    try:
                        foo = Image.open(jpg_fi.path)
                        foo.save(sample_file, optimize=True, quality=quality)
                        w = Path(sample_file)
                        total_before += jpg_fi.size
                        total_after += w.stat().st_size
                    except PIL.UnidentifiedImageError:
                        num_files-=1
                        pass
            if num_files:
                dr_ratio = round(100*(total_before-total_after)/total_before)
                print("Directory {}: {} jpg files, size {} -> {} ({}%), quality {}".format(
                    dir_path, num_files,
                    fmtnum(total_before), fmtnum(total_after),dr_ratio, quality))
        if os.path.exists(sample_file):
            os.remove(sample_file)
        print("=====================================================")


#######################################################################################
# Main
#######################################################################################
if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option("-p", "--path", dest="rootpath",
                      action="store", type="string",
                      help="The root path for analyzing", metavar="rootpath")
    parser.add_option("-z", "--zip", dest="zip",
                      action="store_true",
                      help="Data reduction using zip report", metavar="zip")
    parser.add_option("-j", "--jpg", dest="jpg_quality",
                      action="store", type="int",
                      help="Data reduction using jpg quality reduce", metavar="quality")

    (options, args) = parser.parse_args()
    if not options.rootpath:
        parser.error("rootpath must be provided")

    try:
        print(options)
        fm = FileMgmt(options.rootpath)
        ##pdb.set_trace()
        if options.zip:
            fm.compress_report()
        if options.jpg_quality:
            fm.jpg_quality_reduce_report(options.jpg_quality)
    except:
        traceback.print_exc()
    finally:
        print("Done")

