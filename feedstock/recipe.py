import apache_beam as beam
import pandas as pd

from numcodecs.abc import Codec
from pcodec import auto_compress, auto_decompress

from pangeo_forge_recipes.patterns import ConcatDim, FilePattern
from pangeo_forge_recipes.transforms import OpenURLWithFSSpec, OpenWithXarray, StoreToZarr


# This codec should be added to numcodecs
class Pcodec(Codec):
    def __init__(self):
        pass
        
    def encode(self, buf):
        return auto_compress(buf)

    def decode(self, buf):
        return auto_decompress(buf)


dates = [
    d.to_pydatetime().strftime('%Y%m%d')
    for d in pd.date_range("1996-10-01", "1999-02-01", freq="D")
]

def make_url(time):
    url_base = "https://storage.googleapis.com/pforge-test-data"
    return f"{url_base}/gpcp/v01r03_daily_d{time}.nc"


concat_dim = ConcatDim("time", dates, nitems_per_file=1)
pattern = FilePattern(make_url, concat_dim)
encoding={"time": {"compressor": Pcodec()}}

recipe = (
    beam.Create(pattern.items())
    | OpenURLWithFSSpec()
    | OpenWithXarray(file_type=pattern.file_type, xarray_open_kwargs={"decode_coords": "all"})
    | StoreToZarr(
        store_name="gpcp-pcodec",
        combine_dims=pattern.combine_dim_keys,
        to_zarr_kwargs={"encoding": encoding}
    )
)

#zarr.Blosc(cname="zstd", clevel=3, shuffle=2) # Pcodec
