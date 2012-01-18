#!/usr/bin/python

import unittest
import riak
from riakkit import Document, EmDocument
from riakkit.types import *
rc = riak.RiakClient()

def deleteAllKeys(client, bucketname):
  bucket = client.bucket(bucketname)
  keys = bucket.get_keys()
  for key in keys:
    bucket.get(key).delete()

class CustomDocument(Document):
  client = rc

class E(EmDocument):
  name = ListProperty(forwardprocessors=lambda x: [int(i) for i in x])

class A(CustomDocument):
  bucket_name = "test_A"

  n = StringProperty()
  l = ListProperty(forwardprocessors=lambda x: [int(i) for i in x])
  em = EmDocumentsDictProperty(emdocument_class=E)

class B(CustomDocument):
  bucket_name = "test_B"

  someA = DictReferenceProperty(reference_class=A)

class OtherTests(unittest.TestCase):
  def test_referenceModifiedOnSave(self):
    a = A(l=["1", "2", "3"], em={"a" : {"name" : ["1", "2", "3"]}})
    a.save()
    self.assertEqual(["1", "2", "3"], a.l)
    self.assertEqual(["1", "2", "3"], a.em["a"].name)

  def test_setUpdictRefProperty(self):
    a = A(n="a")
    a.save()
    c = A(n="c")
    b = B(someA={"a" : a, "c" : c})
    b.save()
    b.reload()
    self.assertEqual(b.someA["a"], a)
    self.assertEqual(b.someA["c"], c)

if __name__ == "__main__":
  try:
    import doctest
    print "Running doctests from README.md ..."
    failures, attempts = doctest.testfile("README.md")
    print "Ran through %d tests with %d failures." % (attempts, failures)
    print
    if not failures:
      print "Running unittests..."
      unittest.main()
    else:
      print "Doctest failure, fix those errors first!"
  finally:
    print "Cleaning up..."

    # Clean up
    buckets_to_be_cleaned = ("test_blog", "test_users", "test_comments", "demos",
        "test_website", "coolusers", "_CoolUser_ul_username", "testdoc",
        "test_person", "test_cake", "some_extended_bucket", "test_A", "test_B")

    for bucket in buckets_to_be_cleaned:
      deleteAllKeys(rc, bucket)
