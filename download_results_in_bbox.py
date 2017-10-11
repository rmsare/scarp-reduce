import boto

def form_results_name(code, llx, lly):
    """
    Form valid dataset name using OpenTopography bulk raster naming convention
    for EarthScope data.
    """

    return code + str(llx) + '_' + str(lly) + '_results.tif'

if __name__ == "__main__":
    dest_dir = '/home/rmsare/r/scarplet/raw/'
    ulx = 514
    uly = 4215
    lrx = 527
    lry = 4194

    coords = [(x, y) for x in range(ulx, lrx) for y in range(lry, uly)]
    filenames = [form_results_name('fg', x, y) for x, y in coords]

    connection = boto.connect_s3()
    bucket = connection.get_bucket('scarp-results')

    for f in filenames:
        key = bucket.get_key(f)
        if key:
            key.get_contents_to_filename(dest_dir + f)

