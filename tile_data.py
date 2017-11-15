import os, sys
import rasterio

import numpy as np

def define_tile_transform(transform, row_idx, col_idx):
    dx = transform[1]
    dy = np.abs(transform[5])
    
    ulx = transform[0] + col_idx * dx
    uly = transform[3] + row_idx * dy

    return (ulx, dx, 0, uly, 0, -1*dy)

def tile_data(filename, nx, ny, code='tiles/fg'):
    with rasterio.open(filename, 'r') as dataset:
        data = dataset.read(1)
        num_rows, num_cols = data.shape
        
        transform = dataset.transform
        ulx = transform[0]
        uly = transform[3]
        dx = transform[1]
        dy = np.abs(transform[5])

        for i in range(0, num_rows, ny):
            for j in range(0, num_cols, nx):
                llx = ulx + j * dx
                lly = uly + i * dy
                out_filename = code + str(int(llx / 1000)) + '_' + str(int(lly / 1000)) + '.tif'
                new_transform = define_tile_transform(transform, i, j) 
                
                with rasterio.open(out_filename, 
                        driver='GTiff',
                        width=nx,
                        height=ny, 
                        count=1,
                        crs=dataset.crs, 
                        transform=new_transform, 
                        dtype=rasterio.float32,
                        mode='w') as out:
                    if i + ny < dataset.height and j + nx < dataset.width:
                        out.write(data[i:i + ny, j:j + nx].astype(rasterio.float32), 1)

if __name__ == "__main__":
    filename = sys.argv[1]
    nx = int(sys.argv[2])
    ny = int(sys.argv[3])

    if not os.path.exists('tiles'):
        os.mkdir('tiles')

    tile_data(filename, nx, ny)

