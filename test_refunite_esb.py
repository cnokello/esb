#!/usr/bin/env python
"""App: This is the assembly point for the ETL pipelines
   @author: okello
   @created: 12-aug-2015

"""
import sys
import os
sys.path.append(os.path.abspath("lib"))

from refunite_etl.cfg import Cfg
from refunite_etl.transformers.db_transformer import DBTransformer


if __name__ == "__main__":
    app_args = sys.argv
    if len(app_args) < 2:
        raise Exception("Required arguments not provided. Include location of config file.")
    
    configurator = Cfg()
    DBTransformer(configurator.get_cfg(os.path.abspath(app_args[1]))).run()
    