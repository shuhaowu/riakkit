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
from riakkit.types import BaseProperty, LinkedDocuments, ReferenceBaseProperty, ReferenceProperty, MultiReferenceProperty

from riakkit.exceptions import *

# Last name alphabetical. Email required.
__authors__ = [
    '"Shuhao Wu" <admin@thekks.net>'
]

VERSION = "0.2.0a"

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

  @staticmethod
  def _getProperty(name, attrs, parents):
    value = attrs.get(name, None)
    i = 0
    length = len(parents)
    while value is None:
      if i == length:
        raise RiakkitError("No suitable client found for %s." % name)
      value = getattr(parents[i], name, None)
      i += 1

    return value

  def __new__(cls, clsname, parents, attrs):
    # Makes sure these classes are not registered.

    if clsname == "Document" or "bucket_name" not in attrs:
      return type.__new__(cls, clsname, parents, attrs)

    client = DocumentMetaclass._getProperty("client", attrs, parents)

    attrs["client"] = client
    attrs["bucket"] = client.bucket(attrs["bucket_name"])
    if DocumentMetaclass._getProperty("SEARCHABLE", attrs, parents):
      attrs["bucket"].enable_search()

    meta = {}
    links = {}
    references = {}
    hasdefaults = {}
    uniques = []

    for name in attrs.keys():
      if name in ("_links", "_references"):
        raise RuntimeError("_links is not allowed.")
      if isinstance(attrs[name], LinkedDocuments):
        links[name] = prop = attrs.pop(name)
        if prop.collection_name:
          if prop.collection_name in prop.reference_class._meta["_links"] or prop.collection_name in prop.reference_class._meta["_references"]:
            raise RiakkitError("%s already in %s!" % (prop.collection_name, prop.reference_class))
          prop.reference_class._meta["_links"][prop.collection_name] = LinkedDocuments(reference_class=cls)
      elif isinstance(attrs[name], ReferenceBaseProperty):
        references[name] = prop = attrs.pop(name)
        if prop.collection_name:
          if prop.collection_name in prop.reference_class._meta["_links"] or prop.collection_name in prop.reference_class._meta["_references"]:
            raise RiakkitError("%s already in %s!" % (prop.collection_name, prop.reference_class))
          prop.reference_class._meta["_references"][prop.collection_name] = MultiReferenceProperty(reference_class=cls)
      elif isinstance(attrs[name], BaseProperty):
        meta[name] = prop = attrs.pop(name)
        if prop.unique:
          prop.unique_bucket = attrs["client"].bucket(
              getUniqueListBucketName(attrs["bucket_name"], name)
          )
          uniques.append(name)

        if prop.default is not None:
          hasdefaults[name] = prop.defaultValue()

    meta["_links"] = links
    meta["_references"] = references
    attrs["_meta"] = meta
    attrs["_uniques"] = uniques
    attrs["_hasdefaults"] = hasdefaults
    attrs["instances"] = {}
    new_class = type.__new__(cls, clsname, parents, attrs)

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

  def __init__(self, key=lambda kwargs: uuid1().hex, saved=False, **kwargs):
    """Creates a new document from a bunch of keyword arguments.

    Args:
      key: A string/unicode key or a function that returns a string/unicode key.
           The function takes in 1 argument, and that argument is the kwargs
           that's passed in. Defaults to a lambda function that returns
           uuid1().hex

      saved: Is this object already saved? True or False
      kwargs: Keyword arguments that will fill up the object with data.
    """

    if callable(key):
      self.__dict__["key"] = key(kwargs)
    elif isinstance(key, (str, unicode)):
      self.__dict__["key"] = key
    else:
      raise RiakkitError("%s is not a proper key!" % key)

    self._saved = saved
    self._obj = self.bucket.get(self.key) if saved else None
    self._links = {}
    self._data = {}

    self.mergeData(kwargs)

    self.__class__.instances[self.key] = self


  def mergeData(self, data):
    """Merges data

    Args:
      data: A dictionary of the data to be merged into the object.
    """
    for name in self._hasdefaults:
      if self.__getattr__(name) is None:
        self.__setattr__(name, self._hasdefaults[name])

    for name in data:
      self.__setattr__(name, data[name])

  def equals(self, other):
    return self.key == other.key if type(self) == type(other) else False

  @classmethod
  def _cleanupDataFromDatabase(cls, data):
    if "key" in data:
      del data["key"]

    for k in data:
      if k in cls._meta:
        data[k] = cls._meta[k].convertFromDb(data[k])

    return data

  @classmethod
  def load(cls, riak_obj, cached=False):
    """Construct a Document based object given a RiakObject.

    Args:
      riak_obj: The RiakObject that the document is suppose to build from.
      cached: If we want to get from the pool of objects or not.
              Note that if you have multiple instances of the same object,
              the most recent one to be cached will be used. If this is true,
              riak_obj could just be a string that's the key of the object.
              If not found in cache, it will be fetched from the db.

    Returns:
      A Document object (whichever subclass this was called from).
    """

    if cached:
      if isinstance(riak_obj, RiakObject):
        key = riak_obj.get_key()
      else:
        key = riak_obj # Assume string

      try:
        obj = cls.instances[key]
      except KeyError:
        pass # This should allow it to go to the bottom ones and get the obj.
      else:
        return obj

    if isinstance(riak_obj, str):
      riak_obj = cls.bucket.get(riak_obj)

    if not riak_obj.exists():
      raise NotFoundError("%s not found!" % riak_obj.get_key())

    data = cls._cleanupDataFromDatabase(riak_obj.get_data())
    obj = cls(riak_obj.get_key(), saved=True, **data)
    links = riak_obj.get_links()
    obj._links = obj.updateLinks(links)
    obj._obj = riak_obj
    cls.instances[obj.key] = obj
    return obj

  def updateLinks(self, links):
    """Take a list of RiakLink objects and outputs tag : [Document]

    You probably won't need this function as it's used internally and marked
    public in case something is required.

    However, this function does not convert those reference properties.

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
      obj = link.get()
      cls = _document_classes[link.get_bucket()]
      tag = link.get_tag()
      if tag != last_tag:
        l[tag] = []
        last_tag = tag

      l[tag].append(cls.load(obj, True))  # TODO: Is this a nasty hack?
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

    if name in self._meta["_links"]:
      return self._links.get(name, [])

    inMeta = name in self._meta
    inData = name in self._data

    if not inMeta and inData:
      return self._data[name]

    if inMeta and not inData:
      return None

    if inMeta and inData:
      return self._data[name]

    if not inMeta and not inData:
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
          value = self._meta[name].standardize(value)
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

  def save(self, w=None, dw=None):
    """Saves the document into the database.

    This will save the object to the database. All linked objects will be saved
    as well.

    Args:
      w: W value
      dw: DW value
    """
    data_to_be_saved = {}

    # Process regular data
    for name in self._meta:
      if name in ("_links", "_references"):
        continue

      if name not in self._data:
        if self._meta[name].required:
          raise AttributeError("'%s' is required for '%s'." % (name, self.__class__.__name__))
        data_to_be_saved[name] = None
      else:
        if self._meta[name].unique:
          if self._meta[name].unique_bucket.get(self._meta[name].convertToDb(self._data[name])).exists():
            raise ValueError("'%s' already exists for '%s'!" % (self._data[name], name))

        data_to_be_saved[name] = self._meta[name].convertToDb(self._data[name])

    # Processes the data that's not in the meta
    for name in self._data:
      if name not in data_to_be_saved:
        data_to_be_saved[name] = self._data[name]

    other_docs_to_be_saved = []

    # Process References
    for name in self._meta["_references"]:
      if name not in self._data:
        if self._meta["_references"][name].required:
          raise AttributeError("'%s' is required for '%s'." % (name, self.__class__.__name__))
        data_to_be_saved[name] = None
      else:
        data_to_be_saved[name] = self._meta["_references"][name].convertToDb(self._data[name])
        col_name = self._meta["_references"][name].collection_name
        if isinstance(self._meta["_references"][name], ReferenceProperty):
          docs = [self._data[name]]
        else:
          docs = self._data[name]

        if col_name:
          for doc in docs:
            current_list = doc._data.get(col_name, [])
            current_list.append(self)
            doc._data[col_name] = current_list
            other_docs_to_be_saved.append(doc)

    # Process object
    if self._obj:
      for link in self._obj.get_links():
        self._obj.remove_link(link)
      self._obj.set_data(data_to_be_saved)
    else:
      self._obj = self.bucket.new(self.key, data_to_be_saved)

    for name in self._meta["_links"]:
      if name not in self._links: # TODO: REFACTOR THIS
        if self._meta["_links"][name].required:
          raise AttributeError("'%s' is required for '%s'." % (name, self.__class__.__name__))
        else:
          self._links[name] = []

      col_name = self._meta["_links"][name].collection_name
      for doc in self._links[name]:
        if not isinstance(doc, Document):
          raise AttributeError("%s is not a Document instance!" % item)
        else:
          if col_name:
            current_list = doc._links.get(col_name, [])
            current_list.append(self)
            doc._links[col_name] = current_list
            other_docs_to_be_saved.append(doc)

          if doc._obj is None:
            raise RiakkitError("Add link failure as %s does not exist in the database." % str(doc))
          self._obj.add_link(doc._obj, name) # TODO: doc._obj=None

    self._obj.store(w=w, dw=dw)
    for name in self._uniques:
      obj = self._meta[name].unique_bucket.new(self._data[name], {"key" : self.key})
      obj.store()

    self._saved = True

    for doc in other_docs_to_be_saved:
      doc.save(w, dw)

  def reload(self, r=None, vtag=None):
    """Reloads the object from the database.

    This grabs the most recent version of the object from the database and
    updates the document accordingly. The data will change if the object
    from the database has been changed at one point.

    This only works if the object has been saved at least once before.

    Raises:
      NotFoundError: if the object hasn't been saved before.
    """
    if self._obj:
      self._obj.reload(r=r, vtag=vtag)
      data = self._cleanupDataFromDatabase(self._obj.get_data())
      self.mergeData(data)
      links = self._obj.get_links()
      self._links = self.updateLinks(links)
      self._saved = True
    else:
      raise NotFoundError("Object not saved!")

  def delete(self, rw=None):
    def deleteBackRef(col_name, docs, link):
      if col_name:
        for doc in docs:
          if link:
            current_list = doc._links.get(col_name, [])
          else:
            current_list = doc._data.get(col_name, [])

          modified = False
          for i, linkback in enumerate(current_list):
            if linkback.key == self.key:
              modified = True
              current_list.pop(i) # This is a reference, which should modify the original list.

          if modified:
            doc.save()

    if self._obj is not None:
      for name in self._meta["_links"]:
        deleteBackRef(self._meta["_links"][name].collection_name, self._links.get(name, []), True)
      for name in self._meta["_references"]:
        docs = self._data.get(name, [])
        if isinstance(docs, Document):
          docs = [docs]
        deleteBackRef(self._meta["_references"][name].collection_name, docs, False)

      if self.key in self.__class__.instances:
        del self.__class__.instances[self.key]
      self._obj.delete(rw=rw)
      self._obj = None
      self._saved = False



  @classmethod
  def getWithKey(cls, key, r=None):
    """Get an object with a given key.

    Args:
      key: The key value.
      r: The r value, defaults to None.
    """
    riak_obj = cls.bucket.get(key, r)
    if not riak_obj.exists():
      raise NotFoundError("Key '%s' not found!" % key)
    return cls.load(riak_obj)

  get_with_key = getWithKey

  @classmethod
  def search(cls, querytext):
    """Searches through the bucket with some query text.

    The bucket must have search installed via search-cmd install BUCKETNAME. The
    class must have been marked to be SEARCHABLE with cls.SEARCHABLE = True.

    Args:
      querytext: The query text as outlined in the python-riak documentations.

    Returns:
      A MapReduceQuery object. Similar to the RiakMapReduce object.

    Raises:
      NotImplementedError: if the class is not marked SEARCHABLE.
    """
    if not cls.SEARCHABLE:
      raise NotImplementedError("Searchable is disabled, this is therefore not implemented.")
    query_obj = cls.client.search(cls.bucket_name, querytext)
    return MapReduceQuery(cls, query_obj)

  @classmethod
  def solrSearch(cls, querytext, **kwargs):
    if not cls.SEARCHABLE:
      raise NotImplementedError("Searchable is disabled, this is therefore not implemented.")
    return SolrQuery(cls, cls.client.solr().search(cls.bucket_name, querytext, **kwargs))

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


class SolrQuery(object):
  """A wrapper around RiakSearch to play nice with Document and Solr

  Attributes:
    cls: The class for this SolrQuery
    result: The result dictionary.
  """
  def __init__(self, cls, result):
    self.cls = cls
    self.result = result
    self.loadDoc = lambda doc : self.cls.load(self.cls.bucket.get(doc[u"id"]))

  def length(self):
    """Gets the length of the documents that's searched through."""
    #return self.result[u"response"][u"numFound"]
    return len(self.result[u"response"][u"docs"])

  def run(self):
    """Returns a generator that goes through each document that's searched."""
    for doc in self.result[u"response"][u"docs"]:
      yield self.loadDoc(doc)

  def all(self):
    """Returns all the items that's found and return it.

    Return:
      A list of all the Documents.
    """
    return map(self.loadDoc, self.result[u"response"][u"docs"])


class MapReduceQuery(object):
  """A wrapper around RiakMapReduce to play nice with Document

  Attributes:
    cls: The class for this MapReduceQuery.
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

