#!/usr/bin/env python

from collections import defaultdict

import json

import os
import os.path

import sys

def ensuredir(d):
  if os.path.isfile(d):
    raise Exception("%s exists and is not a directory" % d)
  os.path.isdir(d) or os.mkdir(d)

def preproc_one_file(fn):
  def dd_app(h, s):
    h[s['_type']].append(s)
    return h
  
  def outfn(infn, kind, index):
    suffix = "-%s-%d.json" % (kind, index)
    return infn.replace(".json", suffix)
  
  with open(fn, "r") as js:
    struct = json.load(js)
    by_kind = reduce(dd_app, struct, defaultdict(lambda: []))
    for kind in by_kind:
      index = 0
      ensuredir(kind)
      for record in by_kind[kind]:
        with open("%s/%s" % (kind, outfn(fn, kind, index)), "w") as out:
          json.dump(record, out)
        index = index + 1
      
if __name__ == '__main__':
  for f in sys.argv[1:]:
    print "processing %s..." % f
    preproc_one_file(f)
