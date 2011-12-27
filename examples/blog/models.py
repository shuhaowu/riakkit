from riakkit import Document
from riakkit.types import *
import riak
from hashlib import sha1
from settings import SALT
import string

def hashPassword(password):
  return sha1(password + SALT).hexdigest()

the_client = riak.RiakClient()

class CustomDocument(Document):
  client = the_client

class User(CustomDocument):
  bucket_name = "users"
  SEARCHABLE = True

  # The validator checks for space.
  username = StringProperty(required=True, validators=lambda x: " " not in x)
  password = StringProperty(required=True)
  dispname = StringProperty()

class Post(CustomDocument):
  bucket_name = "blogposts"

  # Title is restricted due to the search operation... for now

  title = StringProperty(required=True, validators=lambda x: len(x) > 0 and x[0] in string.ascii_letters)
  content = StringProperty(required=True)
  mdsrc = StringProperty(required=True)
  date = DateTimeProperty()
  owner = LinkedDocuments(reference_class=User, collection_name="posts")

  SEARCHABLE = True

class Comment(CustomDocument):
  bucket_name = "comments"

  # email address
  who = StringProperty(required=True)
  ip = StringProperty(required=True)
  dispname = StringProperty(required=True)
  website = StringProperty()
  title = StringProperty(required=True, validators=lambda x: len(x) < 256)
  content = StringProperty(required=True)
  post = LinkedDocuments(reference_class=Post, collection_name="comments")
