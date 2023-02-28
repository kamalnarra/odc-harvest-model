import boto3
import rasterio as rio
from rasterio.io import MemoryFile
from rasterio.warp import reproject, Resampling, calculate_default_transform
import pyspatialml
from pyspatialml import Raster


def coregister(template, bucket, file_name):
    with rio.open(f"s3://{bucket}/{file.key}") as input:
        with rio.open(template) as template:
            out_crs = template.crs
            out_transform, out_width, out_height = calculate_default_transform(
                input.crs,
                out_crs,
                template.width,
                template.height,
                *template.bounds,
            )
        out_kwargs = input.meta.copy()
        out_kwargs.update({
            "crs": out_crs,
            "transform": out_transform,
            "width": out_width,
            "height": out_height,
            "nodata": 0
        })
        with MemoryFile() as temp:
            with temp.open(**out_kwargs) as temp_data:
                for i in range(1, input.count + 1): 
                    reproject(
                        source = rio.band(input, i),
                        destination = rio.band(temp_data, i),
                        src_transform = input.transform,
                        src_crs = input.crs,
                        dst_transform = out_transform,
                        dst_crs = out_crs,
                        resampling = Resampling.nearest
                    )      
            s3 = boto3.client('s3')
            s3.upload_fileobj(temp, bucket, file_name)
            print(file_name)

bucket = "harvest-soc-features"
feature_list = []
s3 = boto3.resource('s3')
feature_bucket = s3.Bucket(bucket)
for file in feature_bucket.objects.all():
    #coregister("template.tif", bucket, file.key)
    feature_list.append(f"s3://{bucket}/{file.key}")
