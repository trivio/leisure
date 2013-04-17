# -*- coding: utf-8 -*-
"""
    leisure
    ~~~~~~~~

    Leisure a local job runner for Disco based project.

    It provides a useful method for running your disco project without
    needing a full disco cluster.  This makes it a snap to develop and
    debug jobs on your development machine. 

    To use, simply execute your disco script using the leisure 
    command like so:

      $ leisure <path to script>/word_count.py

    Leisure monkey patches all network calls to the Disco/DDFS master
    so that it can intercept and execute them locally. The  
    worker itself is executed as a subprocess and communicated to via
    the Disco Worker protocol
    http://discoproject.org/doc/disco/howto/worker.html


    :copyright: (c) 2011 by triv.io, see AUTHORS for more details.
    :license: MIT, see LICENSE for more details.
"""

from __future__ import absolute_import
import sys

from .disco import run_script
from . import shuffle

import tempfile

def main():
  script = sys.argv[1]
  run_script(script, tempfile.mkdtemp())


if __name__ == "__main__":
  main()
