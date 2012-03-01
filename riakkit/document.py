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

from riak.mapreduce import RiakLink, RiakObject

from riakkit.queries import *
from riakkit.types import *
from riakkit.exceptions import *
from riakkit.utils import *

from copy import copy, deepcopy
import json
from uuid import uuid1

_document_classes = {}

def getUniqueListBucketName(class_name, property_name):
  """Gets the bucket name that enforces the uniqueness of a certain property.

  Args:
    class_name: The name of the class
    property_name: The property name

  Returns:
    Returns the bucket name.
  """
  return "_%s_ul_%s" % (class_name, property_name)


class DocumentMetaclass(type):
  """Meta class that the Document class is made from.

  Checks for bucket_name in each class, as those are necessary.
  """

  def __new__(cls, clsname, parents, attrs):
    client = getProperty("client", attrs, parents)

    if client is None:
      return type.__new__(cls, clsname, parents, attrs)

    attrs["client"] = client
    meta = {}
    uniques = []
    references_col_classes = []

    for name in attrs.keys():
      if isinstance(attrs[name], BaseProperty):
        meta[name] = prop = attrs.pop(name)
        if hasattr(prop, "collection_name") and prop.collection_name:
          if prop.collection_name in prop.reference_class._meta:
            raise RiakkitError("%s already in %s!" % (prop.collection_name, prop.reference_class))
          references_col_classes.append((prop.collection_name, prop.reference_class, name))
        elif prop.unique:
          prop.unique_bucket = attrs["client"].bucket(
              getUniqueListBucketName(clsname, name)
          )
          uniques.append(name)

    # DUPLICATE WORK WITH getProperty
    all_parents = reversed(walkParents(parents))
    for p_cls in all_parents:
      meta.update(p_cls._meta)
      uniques.extend(p_cls._uniques)

    attrs["_meta"] = meta
    attrs["_uniques"] = uniques
    attrs["SEARCHABLE"] = getProperty("SEARCHABLE", attrs, parents)
    attrs["instances"] = {}

    new_class = type.__new__(cls, clsname, parents, attrs)

    if "bucket_name" in attrs:
      if new_class.bucket_name in _document_classes:
        raise RiakkitError("Bucket name of %s already exists in the registry!"
                              % new_class.bucket_name)
      else:
        _document_classes[new_class.bucket_name] = new_class

      new_class.bucket = client.bucket(new_class.bucket_name)
      if new_class.SEARCHABLE:
        new_class.bucket.enable_search()

    for col_name, rcls, back_name in references_col_classes:
      # col_name is the collection name from this class
      # back_name is the name that points from the original class.
      rcls._meta[col_name] = MultiReferenceProperty(reference_class=new_class)
      rcls._meta[col_name].is_reference_back = back_name

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
      key = key(kwargs)

    if not isinstance(key, (str, unicode)):
      raise RiakkitError("%s is not a proper key!" % key)

    if key in self.__class__.instances:
      raise RiakkitError("%s already exists! Use get instead!" % key)

    self.__dict__["key"] = key

    self._obj = self.bucket.get(self.key) if saved else None

    self._data = {}
    for k, prop in self._meta.iteritems():
      if not prop.required:
        self._data[k] = prop.defaultValue()

    self._links = {}
    self._indexes = {}

    self.mergeData(kwargs)

    self._saved = saved
    # self._storeOriginals()

    self.__class__.instances[self.key] = self


  def mergeData(self, data):
    """Merges data. This will trigger the standard processors.

    Args:
      data: A dictionary of the data to be merged into the object.
    """
    for key, value in data.iteritems():
      self.__setattr__(key, value)

  @classmethod
  def _getIndexesFromObj(cls, obj): # TODO: These really needs to get some sort of coordination. Should be static? naming? ..etc...
    obj_indexes = obj.get_indexes()
    indexes = {}
    for index_entry in obj_indexes:
      field = index_entry.get_field()
      value = index_entry.get_value()
      l = indexes.get(field, [])
      l.append(value)
      indexes[field] = l
    return indexes

  @classmethod
  def _getLinksFromObj(cls, obj):
    return {}

  @classmethod
  def _cleanupDataFromDatabase(cls, data):
    keys = getKeys(data, cls._meta)
    for k in keys:
      if k in cls._meta:
        if k in data:
          data[k] = cls._meta[k].convertFromDb(data[k])
        else:
          data[k] = cls._meta[k].defaultValue()

    return data

  @classmethod
  def flushDocumentFromCache(cls, k=None):
    """Removes an instance from the instances being tracked.

    A potentially dangerous method. This only flushes riakkit's internal cache.
    Those objects are not destroyed. Any references to it will still work. This
    will break the 1 key per object. Use at your own risk.

    Some potential uses: since a lot of objects could accumulate in RAM, this
    could be used there.

    However, Riakkit is not very optimized as of right now.. perhaps in the
    future it will be. This means, Riakkit needs a more awesome caching backend.

    Args:
      k: A key or a Document instance. If None, it will flush the entire cache
         for this class. Default: None
    """

    if k is None:
      k = cls.instances.keys()

    if isinstance(k, cls):
      k = [k.key]
    elif isinstance(k, (unicode, str)):
      k = [k]

    for key in k:
      cls.instances.pop(key)

  def addIndex(self, field, value):
    """Adds an index to the document for Riak 2i.

    eleveldb must be enabled instead of bitcask

    Args:
      field: The index field
      value: The index value
    """

    l = self._indexes.get(field, [])
    l.append(value)
    self._indexes[field] = l

  def removeIndex(self, field, value):
    """Removes an index from the document

    Args:
      field: The field name
      value: The value

    Raises:
      ValueError: If field, value is not found..
    """
    l = self._indexes.get(field, [])
    try:
      l.remove(value)
    except ValueError:
      raise ValueError("%s: %s index not found!" % (field, value))
    else:
      self._indexes[field] = l

  def getIndexes(self, field=None):
    """Gets the indexes.

    Args:
      field: The field name. If it is None, all the indexes will be returned in
             a dictionary. Otherwise it will return the list of index values.
             Default: None
    Returns:
      If field is None this returns a dictionary of field : [value, value]
      Otherwise it returns a list of [value, value]

    Raises:
      KeyError: if field doesn't exists
    """
    if field is None:
      return deepcopy(self._indexes)
    else:
      try:
        return copy(self._indexes[field])
      except KeyError:
        raise KeyError("Index field %s doesn't exist!" % field)

  def setIndexes(self):
    pass

  def addLink(self, doc, tag=None):
    l = self._links.get(tag, [])
    l.append(doc)
    self._links[tag] = l

  def removeLink(self):
    pass

  def getLinks(self, tag=None):
    if tag is None:
      links = []
      for tag in self._links:
        for doc in self._links[tag]:
          links.append((doc, tag))
      return links
    else:
      return self._links.get(tag, [])

  def setLinks(self):
    pass

  @classmethod
  def load(cls, riak_obj, cached=False):
    """Construct a Document based object given a RiakObject.

    Args:
      riak_obj: The RiakObject that the document is suppose to build from.
      cached: Reload the object or not if it's found in the pool of objects.

    Returns:
      A Document object (whichever subclass this was called from).
    """

    if isinstance(riak_obj, RiakObject):
      key = riak_obj.get_key()
    else:
      key = riak_obj

    try:
      obj = cls.instances[key]
    except KeyError:
      if isinstance(riak_obj, (str, unicode)):
        riak_obj = cls.bucket.get(key)
      if not riak_obj.exists():
        raise NotFoundError("%s not found!" % key)
      # This is done before so that _cleanupDataFromDatabase won't recurse
      # infinitely with collection_name. This wouldn't cause an problem as
      # _cleanupDataFromDatabase calls for the loading of the referenced document
      # from cache, which load this document from cache, and it see that it
      # exists, finish loading the referenced document, then come back and finish
      # loading this document.
      obj = cls(riak_obj.get_key(), saved=True)
      cls.instances[obj.key] = obj

      data = cls._cleanupDataFromDatabase(deepcopy(riak_obj.get_data()))
      obj._data = data
      obj._links = cls._getLinksFromObj(riak_obj)
      obj._indexes = cls._getIndexesFromObj(riak_obj)
      obj._obj = riak_obj
      return obj
    else:
      if not cached:
        obj.reload()
      return obj


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

    inMeta = name in self._meta
    inData = name in self._data

    if inData:
      return self._data[name]

    if inMeta and not inData:
      return None

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
      validator = lambda x: True
      standardizer = lambda x: x
      if name in self._meta:
        validator = self._meta[name].validate
        standardizer = self._meta[name].standardize

      if not validator(value):
        raise ValueError("Validation did not pass for %s for the field %s.%s" % (value, self.__class__.__name__, name))
      value = standardizer(value)
      self._data[name] = value

      self._saved = False

  def __delattr__(self, name):
    """Deletes an attribute.

    Args:
      name: The name of the field.
    Raises:
      KeyError: if name is not found in the data set
    """
    del self._data[name]

  def saved(self):
    """See if the object has been saved or not.

    The current implementation checks if the object exists in the database and
    if it has been modified or not.

    This method is bugged when the reference to the object is called and they
    are modified. Example. list.append will not make saved to be False.

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
    uniques_to_be_deleted = []
    other_docs_to_be_saved = []

    keys = getKeys(self._meta, self._data)
    for k in keys:
      inMeta = k in self._meta
      if not inMeta: # in dictionary is O(1). Stop freaking out like you did..
        data_to_be_saved[k] = self._data[k]
        continue

      prop = self._meta[k]
      is_ref = hasattr(prop, "collection_name")

      if k not in self._data:
        if prop.required:
          raise AttributeError("'%s' is required for '%s'." % (k, self.__class__.__name__))
        if prop.unique:
          old = self._obj.get_data().get(k, None)
          if old is not None:
            uniques_to_be_deleted.append((prop.unique_bucket, old))

        self._data[k] = prop.defaultValue()
      else:
        if is_ref:
          col_name = prop.collection_name
          if col_name:
            if self._obj is not None:
              originals = self._obj.get_data()[k]
              if originals is None:
                originals = []
              elif not isinstance(originals, list):
                originals = [originals]
            else:
              originals = []

            if isinstance(prop, ReferenceProperty):
              docs = [self._data[k]]
            else:
              docs = self._data[k]

            dockeys = {} # for fast look up

            for doc in docs:
              if doc is None:
                continue

              dockeys[doc.key] = doc

              current_list = doc._data.get(col_name, [])
              found = False
              for d in current_list:
                if d.key == self.key:
                  found = True
                  break

              if not found:
                current_list.append(self)
                doc._data[col_name] = current_list
                other_docs_to_be_saved.append(doc)

            for dockey in originals:
              if dockey is None:
                continue

              if dockey in dockeys:
                pass
              else:
                try:
                  doc = self._meta[k].reference_class.load(dockey, True)
                except NotFoundError: # TODO: Another hackjob? This is _probably_ due to we're back deleting the reference.
                  continue

                current_list = doc._data.get(col_name, [])
                found = False
                for i, d in enumerate(current_list):
                  if d.key == self.key:
                    current_list.pop(i)
                    found = True
                    break

                if found:
                  doc._data[col_name] = current_list
                  other_docs_to_be_saved.append(doc)

        else:
          if prop.unique:
            changed = False
            if self._obj is not None:
              old = self._obj.get_data()[k]
              if self._data[k] != old and old is not None:
                uniques_to_be_deleted.append((prop.unique_bucket, old))
                changed = True
            else:
              changed = True

            if changed and prop.unique_bucket.get(prop.convertToDb(self._data[k])).exists():
              raise ValueError("'%s' already exists for '%s'!" % (self._data[k], k))

      data_to_be_saved[k] = prop.convertToDb(self._data[k]) # dup work with conversion


    if self._obj:
      self._obj.set_data(data_to_be_saved)
    else:
      self._obj = self.bucket.new(self.key, data_to_be_saved)


    # Handle links.
    links = []
    for tag, l in self._links.iteritems():
      for d in l:
        links.append(RiakLink(self.bucket_name, d.key, tag))

    if hasattr(self._obj, "set_links"): # If you live on my setlinks branch on my repo.
      self._obj.set_links(links, True)
    else:
      for link in self._obj.get_links():
        self._obj.remove_link(link)
      for link in links:
        self._obj.add_link(link)

    indexes = []
    for field, l in self._indexes.iteritems():
      for value in l:
        indexes.append((field, value))

    if hasattr(self._obj, "set_indexes"):
      self._obj.set_indexes(indexes)
    else:
      for index in self._obj.get_indexes():
        self._obj.remove_index(index.get_field(), index.get_value())

      for field, value in indexes:
        self._obj.add_index(field, value)

    self._obj.store(w=w, dw=dw)
    for name in self._uniques:
      obj = self._meta[name].unique_bucket.new(self._data[name], {"key" : self.key})
      obj.store(w=w, dw=dw)

    for bucket, key in uniques_to_be_deleted:
      bucket.get(key).delete()

    self._saved = True
    for doc in other_docs_to_be_saved:
      doc.save(w, dw)

  def reload(self, r=None, vtag=None):
    """Reloads the object from the database.

    This grabs the most recent version of the object from the database and
    updates the document accordingly. The data will change if the object
    from the database has been changed at one point.

    This only works if the object has been saved at least once before.

    Returns:
      self for OOP.

    Raises:
      NotFoundError: if the object hasn't been saved before.
    """

    if self._obj:
      self._obj.reload(r=r, vtag=vtag)
      data = self._cleanupDataFromDatabase(deepcopy(self._obj.get_data()))
      self._data = data
      self._links = self._getLinksFromObj(self._obj)
      self._saved = True
    else:
      raise NotFoundError("Object not saved!")

  def delete(self, rw=None):
    """Deletes this object from the database. Same interface as riak-python.

    However, this object can still be resaved. Not sure what you would do
    with it, though.
    """
    def deleteBackRef(col_name, docs):
      docs_to_be_saved = []
      for doc in docs:
        current_list = doc._data.get(col_name, [])
        modified = False

        if isinstance(current_list, Document): # For collection_name referencing back
          doc._data[col_name] = None
          modified = True
        else:
          for i, linkback in enumerate(current_list):
            if linkback.key == self.key:
              modified = True
              current_list.pop(i) # This is a reference, which should modify the original list.

        if modified:
          docs_to_be_saved.append(doc)

      return docs_to_be_saved

    if self._obj is not None:
      docs_to_be_saved = []
      for k in self._meta:
        col_name = getattr(self._meta[k], "is_reference_back", False)
        if not col_name:
          col_name = getattr(self._meta[k], "collection_name", None)

        if col_name:
          docs = self._data.get(k, [])
          if isinstance(docs, Document):
            docs = [docs]
          docs_to_be_saved.extend(deleteBackRef(col_name, docs))

      try:
        del self.__class__.instances[self.key]
      except KeyError:
        pass

      self._obj.delete(rw=rw)

      for name in self._uniques:
        obj = self._meta[name].unique_bucket.get(self._data[name])
        obj.delete()

      self._obj = None
      self._saved = False

      for doc in docs_to_be_saved:
        doc.save()

  @classmethod
  def exists(cls, key, r=None):
    """Check if a key exists.

    Args:
      key: The key to check if exists or not.
      r: The R value

    Returns:
      True if the key exists, false otherwise.
    """
    return cls.bucket.get(key, r).exists()

  @classmethod
  def get(cls, key, r=None): # TODO: This should be merged with load()
    """Get an object with a given key.

    Args:
      key: The key value.
      r: The r value, defaults to None.
    """
    if key in cls.instances:
      obj = cls.instances[key]
      obj.reload()
      return obj

    riak_obj = cls.bucket.get(key, r)
    if not riak_obj.exists():
      raise NotFoundError("Key '%s' not found!" % key)
    return cls.load(riak_obj)

  getWithKey = get
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
  def index(cls, index, startkey, endkey=None):
    """Short hand for creating a new mapreduce index

    Args:
      index: The index field
      startkey: The starting key
      endkey: The ending key. If not none, search a range. Default: None

    Returns:
      A RiakMapReduce object
    """
    return MapReduceQuery(cls, cls.client.index(cls.bucket_name, index, startkey, endkey))

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
