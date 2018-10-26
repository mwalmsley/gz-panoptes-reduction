import os
import sys
import io
import shutil
import json

import pandas as pd
from pyspark import SparkContext, SparkConf

from gzreduction import settings
from tests import TEST_EXAMPLE_DIR
from gzreduction.panoptes.panoptes_extract_to_votes import load_annotation
# workers will use system python by default - bad! 
# this env variable tells them to use this one
os.environ['PYSPARK_PYTHON'] = sys.executable  # path to current interpreter


appName = 'hello_world'  # name to display
master = 'local[*]'  # don't run on remote cluster. [*] indicates use all cores.

# create launch configuration
conf = SparkConf().setAppName(appName).setMaster(master) 
sc = SparkContext(conf=conf)  # this tells spark how to access a cluster

# create distributed dataset on in-memory iterator with 'parallelize'
# data = [1, 2, 3, 4, 5]
# distData = sc.parallelize(data)
# can now call e.g. distData.reduce(etc)

# or, create on external data (local or S3)
lines = sc.textFile(os.path.join(TEST_EXAMPLE_DIR, 'panoptes_classifications.csv'))

# lineLengths = lines.map(lambda s: len(s))  # can apply any python function. Map is lazy.
# totalLength = lineLengths.reduce(lambda a, b: a + b)  # action, actually runs the computation
# print(totalLength)

# I want to apply a function which takes a classification row, and returns a block of lines
# which match the clean flat csv format I desire


# grab a few classification lines to test on (once only)
with open(settings.panoptes_old_style_classifications_loc, 'r') as input_f:
    lines = [input_f.readline() for line_n in range(10000)]
header_str = lines[0]
lines = lines[1:]
# with open(os.path.join(TEST_EXAMPLE_DIR, 'panoptes_classifications_10k_headerless.csv'), 'w') as output_f:
#     for line in lines:
#         output_f.write(line)

debug = False
if debug:
    exploded_lines = [explode_classification(line, header_str) for line in lines]
    print(exploded_lines[0])
else:
    lines = sc.textFile(os.path.join(TEST_EXAMPLE_DIR, 'panoptes_classifications_10k_headerless.csv'))
    # print(lines.collect()[0])
    exploded_lines = lines.flatMap(lambda x: explode_classification(x, header_str))
    exploded_lines.map(lambda x: sanitise_string(exploded_lines['value']))
    save_loc = os.path.join('spark_playground', 'exploded_classifications.csv')
    shutil.rmtree(save_loc)
    exploded_lines.saveAsTextFile(save_loc)
