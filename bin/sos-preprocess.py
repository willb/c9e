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
  
  def outfn(infn, kind):
    return "%s-%s" % (kind, infn)
  
  with open(fn, "r") as js:
    struct = json.load(js)
    by_kind = reduce(dd_app, struct, defaultdict(lambda: []))
    for kind in by_kind:
      print(" - writing %s records..." % kind)
      ensuredir(kind)
      with open("%s/%s" % (kind, outfn(fn, kind)), "w") as out:
        for record in by_kind[kind]:
          json.dump(record, out)
          out.write("\n")
      
if __name__ == '__main__':
  for f in sys.argv[1:]:
    print("processing %s..." % f)
    preproc_one_file(f)
