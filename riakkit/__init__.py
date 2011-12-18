# This file is part of RiakKit.
#
# RiakKit is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# RiakKit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with RiakKit.  If not, see <http://www.gnu.org/licenses/>.

import json
from uuid import uuid1

import riak
from riak.mapreduce import RiakLink, RiakObject
from riakkit.types import BaseProperty, LinkedDocuments
from riakkit.exceptions import *


# Last name alphabetical. Email required.
__authors__ = [
    '"Shuhao Wu" <admin@thekks.net>'
]

VERSION = "0.1.1a"

_document_classes = {}

def getUniqueListBucketName(bucket_name, property_name):
  """Gets the bucket name that enforces the uniqueness of a certain property.

  Args:
    bucket_name: The bucket name
    property_name: The property name

  Returns:
    Returns the bucket name.
  """
  return "_%s_ul_%s" % (bucket_name, property_name)

class DocumentMetaclass(type):
  """Meta class that the Document class is made from.

  Checks for bucket_name in each class, as those are necessary.
  """

  def __new__(cls, name, parents, attrs):
    if name == "Document" or "bucket_name" not in attrs:
      return type.__new__(cls, name, parents, attrs)

    client = attrs.get("client", None)
    i = 0
    length = len(parents)
    while client is None:
      if i == length:
        raise RiakkitError("No suitable client found for %s." % name)
      client = parents[i].client
      i += 1

    attrs["client"] = client
    attrs["bucket"] = client.bucket(attrs["bucket_name"])

    meta = {}
    links = {}
    uniques = []


    for key in attrs.keys():
      if key == "_links":
        raise RuntimeError("_links is not allowed.")
      if isinstance(attrs[key], LinkedDocuments):
        links[key] = attrs.pop(key)
        if links[key].collection_name:
          links[key].reference_class._meta["_links"][links[key].collection_name] = LinkedDocuments(reference_class=cls)
          links[key].reference_class._collections[links[key].collection_name] = cls

      elif isinstance(attrs[key], BaseProperty):
        meta[key] = attrs.pop(key)
        if meta[key].unique:
          meta[key].unique_bucket = attrs["client"].bucket(
              getUniqueListBucketName(attrs["bucket_name"], key)
          )
          uniques.append(key)

    meta["_links"] = links # Meta. lol
    attrs["_meta"] = meta
    attrs["_uniques"] = uniques
    attrs["_collections"] = {}
    attrs["instances"] = {}

    new_class = type.__new__(cls, name, parents, attrs)
    if new_class.bucket_name in _document_classes:
      raise RiakkitError("Bucket name of %s already exists in the registry!"
                            % new_class.bucket_name)
    else:
      _document_classes[new_class.bucket_name] = new_class

    return new_class

class Document(object):
  """The base Document class for other classes to extend from.

  There are a couple of class variables that needs to be filled out. First is
  client. client is an instance of a RiakClient. The other is bucket_name. This
  is the name of the bucket to be stored in Riak. It must not be shared with
  another Document subclass. Lastly, you may set the SEARCHABLE to True or False

  Class variables that's an instance of the BaseType will be the schema of the
  document.
  """


  __metaclass__ = DocumentMetaclass
  SEARCHABLE = False
  client = None

  def __init__(self, key=None, saved=False, **kwargs):
    """Creates a new document from a bunch of keyword arguments.

    Args:
      key: Set a key.
      saved: Is this object already saved? Key must have something.
      kwargs: Keyword arguments that will fill up the schema.
    """

    self._links = {}
    self._saved = saved and key
    self.__dict__["key"] = key or uuid1().hex
    self._obj = self.bucket.get(self.key) if saved else None
    self._data = {}
    for name in kwargs:

      # Safely merge. self._data.update unreliable

      self.__setattr__(name, kwargs[name])

    self.__class__.instances[self.key] = self

  def __cmp__(self, other):
    """Compares a document with another. Uses key comparison.

    Returns:
      True if the key is the same, otherwise no."""
    return self.key == other.key if type(self) == type(other) else False

  @classmethod
  def _cleanupDataFromDatabase(cls, data):
    if "key" in data:
      del data["key"]

    for k in data:
      if k in cls._meta:
        data[k] = cls._meta[k].convertBack(data[k])

    return data

  @classmethod
  def load(cls, riak_obj, cached=False):  # TODO: Merge this with reload() somehow
    """Construct a Document based object given a RiakObject.

    Args:
      riak_obj: The RiakObject that the document is suppose to build from.
      cached: If we want to get from the pool of objects or not.
              Note that if you have multiple instances of the same object,
              the most recent one to be cached will be used. If this is true,
              riak_obj could just be a string that's the key of the object

    Returns:
      A Document object (whichever subclass this was called from).
    """
    if cached:
      if isinstance(riak_obj, RiakObject):
        key = riak_obj.get_key()
      else:
        key = riak_obj # Assume string

      try:
        return cls.instances[key]
      except KeyError:
        raise NotFoundError("%s not found!" % key)

    data = cls._cleanupDataFromDatabase(riak_obj.get_data())
    obj = cls(riak_obj.get_key(), **data)
    links = riak_obj.get_links()
    obj._links = obj.updateLinks(links)
    obj._saved = True
    obj._obj = riak_obj
    cls.instances[obj.key] = obj
    return obj

  def updateLinks(self, links):
    """Take a list of RiakLink objects and outputs tag : [Document]

    You probably won't need this function as it's used internally and marked
    public in case something is required.

    Args:
      links: A list of RiakLink objects

    Returns:
      This returns a dict that has the tags of the links as keys and a list
      of documents as values. The documents are constructed using the classes
      that is associated with the link's bucket. It's probably not a good idea
      to mix this with links that doesn't have a Document subclass associated to
      it. Example:
      {tag : [Document, Document]}
    """
    l = {}
    links = sorted(links, key=lambda x: x.get_tag())
    last_tag = None
    for link in links:
      cls = _document_classes[link.get_bucket()]
      tag = link.get_tag()
      if tag != last_tag:
        l[tag] = []
        last_tag = tag

      l[tag].append(cls.load(link.get(), True))  # TODO: Is this a nasty hack?
    return l

  def _error(self, name):
    raise AttributeError("Attribute %s not found with %s." %
        (name, self.__class__.__name__))

  def __getattr__(self, name):
    """Returns the data that's associated with that key.

    If the key is defined in the schema but never set, this will return None.
    If the key is not defined in the schema but was set, this will return its
    value. If the key is neither in the schema, nor set, it will raise an error.

    Args:
      name: The field name defined in the schema or added in later.

    Returns:
      The value associated with the name

    Raises:
      AttributeError: When the name is not registered in the schema nor set.
    """
    if name in self._meta or name in self._data: # TODO: in operator does a linear search. Binary search?
      return self._data.get(name, None)
    elif name in self._meta["_links"]:
      return self._links.get(name, None)
    else:
      self._error(name)

  def __setattr__(self, name, value):
    """Sets some value to a field.

    Validation will be done here.

    Args:
      name: The name you're willing to set.
      value: The value that you wish to set.

    Raises:
      ValueError: if validation is not passed.
    """
    if name.startswith("_"):
      self.__dict__[name] = value
    else:
      if name in self._meta["_links"]:
        self._links[name] = value
      else:
        if name in self._meta:
          if not self._meta[name].validate(value):
            raise ValueError("Validation did not pass for %s for the field %s.%s" % (value, self.__class__.__name__, name))

        self._data[name] = value
      self._saved = False

  def __delattr__(self, name):
    """Deletes an attribute.

    Args:
      name: The name of the field.
    """
    if name in self._links:
      del self._links[name]
    elif name in self._data:
      del self._data[name]

  def saved(self):
    """See if the object has been saved or not.

    The current implementation checks if the object exists in the database and
    if it has been modified or not.

    Returns:
      True if saved, False if not.
    """
    return self._obj is not None and self._obj.exists() and self._saved

  def _verifyData(self):
    """Verifies the data.

    This method does not alter the database, but may alter the data.
    """
    for name in self._meta:
      if name == "_links":
        continue

      if name not in self._data: # TODO: REFACTOR THIS
        if self._meta[name].required:
          raise AttributeError("'%s' is required for '%s'." % (name, self.__class__.__name__))
        else:
          self._data[name] = None

      if self._meta[name].unique:
        if self._meta[name].unique_bucket.get(self._meta[name].convert(self._data[name])).exists():
          raise ValueError("'%s' already exists for '%s'!" % (self._data[name], name))

      self._data[name] = self._meta[name].convert(self._data[name])

    for name in self._meta["_links"]:
      if name not in self._links: # TODO: REFACTOR THIS
        if self._meta["_links"][name].required:
          raise AttributeError("'%s' is required for '%s'." % (name, self.__class__.__name__))
        else:
          self._links[name] = []

      for item in self._links[name]:
        if not isinstance(item, Document):
          raise AttributeError("%s is not a Document instance!" % item)

  def reload(self):
    """Reloads the object from the database.

    This grabs the most recent version of the object from the database and
    updates the document accordingly. The data will change if the object
    from the database has been changed at one point.

    This only works if the object has been saved at least once before.

    Raises:
      NotFoundError: if the object hasn't been saved before.
    """
    if self._obj:
      self._obj.reload()
      data = self._cleanupDataFromDatabase(self._obj.get_data())
      self._data = data # TODO: Is this safe?
      links = self._obj.get_links()
      self._links = self.updateLinks(links)
      self._saved = True
    else:
      raise NotFoundError("Object not saved!")

  def save(self):
    """Saves the document into the database.

    This will save the object to the database. All linked objects will be saved
    as well.
    """
    bucket = self.bucket
    if self.SEARCHABLE:
      bucket.enable_search()

    self._verifyData()

    if self._obj is None:
      self._obj = bucket.new(self.key, self._data)
    else:
      self._obj.set_data(self._data)

    for link in self._obj.get_links():
      self._obj.remove_link(link)

    for tag in self._links:
      docs = self._links[tag]
      col_name = self._meta["_links"][tag].collection_name
      if col_name:  # This type of non-refactored is done for efficiency's sake
        for i, doc in enumerate(docs):
          current_list = doc._links.get(col_name, [])  # TODO: Un-uglify this
          current_list.append(self)
          doc._links[col_name] = current_list
          if tag not in self._collections: # _collections is signifying which attributes is generated via collection_name by other Document classes
            doc.save()  # Stack too big here? Nah. Let's take the leap of faith.
          self._obj.add_link(doc._obj, tag) # TODO: Maybe some sort of ordering, with tag0, tag1.. etc
      else:
        for i, doc in enumerate(docs):
          if tag not in self._collections and not doc.saved():
            doc.save()
          self._obj.add_link(doc._obj, tag) # TODO: Maybe some sort of ordering, with tag0, tag1.. etc


    self._obj.store()
    self.key = self._obj.get_key()
    for name in self._uniques:
      obj = self._meta[name].unique_bucket.new(self._data[name], {"key" : self.key})
      obj.store()

    self._saved = True

  def delete(self):
    """This will delete the object from the database."""
    if self._obj is not None:
      self._obj.delete()
      self._obj = None
      self._saved = False

  @classmethod
  def get_with_key(cls, key, r=None):
    """Get an object with a given key.

    Args:
      key: The key value.
      r: The r value, defaults to None.
    """
    riak_obj = cls.bucket.get(key, r)
    if not riak_obj.exists():
      raise NotFoundError("Key '%s' not found!" % key)
    return cls.load(riak_obj)

  @classmethod
  def search(cls, querytext):
    """Searches through the bucket with some query text.

    The bucket must have search installed via search-cmd install BUCKETNAME. The
    class must have been marked to be SEARCHABLE with cls.SEARCHABLE = True.

    Args:
      querytext: The query text as outlined in the python-riak documentations.

    Returns:
      A RiakkitQuery object. Similar to the RiakMapReduce object.

    Raises:
      NotImplementedError: if the class is not marked SEARCHABLE.
    """
    if not cls.SEARCHABLE:
      raise NotImplementedError("Searchable is disabled, this is therefore not implemented.")
    query_obj = cls.client.search(cls.bucket_name, querytext)
    return RiakkitQuery(cls, query_obj)

  @classmethod
  def mapreduce(cls):
    """Shorthand for creating a query object for map reduce.

    Returns:
      A RiakMapReduce object.
    """
    return cls.client.add(cls.bucket_name)

  __getitem__ = __getattr__
  __setitem__ = __setattr__
  __delitem__ = __delattr__


class RiakkitQuery(object):
  """A wrapper around RiakMapReduce to play nice with Document

  Attributes:
    cls: The class for this RiakkitQuery.
    mr_obj: The original RiakMapReduce object.
    riak_links: All the links returned from the run operation of RiakMapReduce.
  """
  def __init__(self, cls, mr_obj):
    self.cls = cls
    self.mr_obj = mr_obj
    self.riak_links = mr_obj.run()

  def run(self):
    """A generator that goes through riak_link"""
    for link in self.riak_links:
      yield self.cls.load(link.get())

  def length(self):
    """The number of objects in this query.

    Return:
      an integer that is the length of riak_obj
    """
    return len(self.riak_links)

  def all(self):
    """Returns all the Documents in a single list.

    Returns:
      A list containing all the Documents
    """
    return map(lambda link: self.cls.load(link.get()), self.riak_links)


