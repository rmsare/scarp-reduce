import os
from shutil import copyfile

def form_dataset_name(code, llx, lly):
    """
    Form valid dataset name using OpenTopography bulk raster naming convention
    for EarthScope data.
    """

    return code + str(llx) + '_' + str(lly) + '.tif'

if __name__ == "__main__":
    source_dir = '/media/rmsare/GALLIUMOS/data/ot_data/tif/'
    dest_dir ='/home/rmsare/r/scarplet/tif/'
    ulx = 514
    uly = 4215
    lrx = 527
    lry = 4194

    coords = [(x, y) for x in range(ulx, lrx) for y in range(lry, uly)]
    filenames = [form_dataset_name('fg', x, y) for x, y in coords]
    filenames = set.intersection(set(filenames), set(os.listdir(source_dir)))

    print("Copying {} files".format(len(filenames)))
    for f in filenames:
        if not os.path.exists(dest_dir + f):
            copyfile(source_dir + f, dest_dir + f) 

