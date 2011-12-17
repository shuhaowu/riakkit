#!/usr/bin/python
import pdb


def deleteAllKeys(client, bucketname):
  bucket = client.bucket(bucketname)
  keys = bucket.get_keys()
  print "Keys to be cleaned in bucket %s: %s" % (bucketname, str(keys))
  for key in keys:
    print ("Deleting %s..." % key), not bucket.get(key).delete().exists()
  print


if __name__ == "__main__":
  import doctest
  print "Running tests...."
  doctest.testfile("README.md")
  print "If nothing happened, success.. otherwise.. DEBUG HOUR!"

  # Clean up
  import riak
  print "Clean up time!"
  client = riak.RiakClient()
  deleteAllKeys(client, "test_blog")
  deleteAllKeys(client, "test_blog")
  deleteAllKeys(client, "test_comments")
  deleteAllKeys(client, "demos")
  deleteAllKeys(client, "coolusers")
  deleteAllKeys(client, "_coolusers_ul_username")
