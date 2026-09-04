#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#


import sys

from airbyte_cdk.entrypoint import launch
from source_convex_data_sync import SourceConvexDataSync


def run():
    source = SourceConvexDataSync()
    launch(source, sys.argv[1:])
