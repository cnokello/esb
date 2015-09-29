#!/usr/bin/env python
"""App: This is the startup script for the EBS pipelines
   @author: okello
   @created: 12-aug-2015

"""
import sys
import os
sys.path.append(os.path.abspath("lib"))

from refunite_etl.app import App



if __name__ == "__main__":
    app_args = sys.argv
    if len(app_args) < 2:
        raise Exception("Required arguments not provided. Include location of config file.")
    
    App(os.path.abspath(app_args[1]))
    