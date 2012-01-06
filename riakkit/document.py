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

from riakkit.queries import *
import json
from uuid import uuid1

from riak.mapreduce import RiakLink, RiakObject
from riakkit.types import BaseProperty, LinkedDocuments, ReferenceBaseProperty, ReferenceProperty, MultiReferenceProperty

from riakkit.exceptions import *
from riakkit.utils import *
from copy import copy, deepcopy

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
    # Makes sure these classes are not registered.

    client = getProperty("client", attrs, parents)
    if client is None:
      return type.__new__(cls, clsname, parents, attrs)

    attrs["client"] = client

    meta = {}
    links = {}
    references = {}
    hasdefaults = {}
    uniques = []

    linked_docs_col_classes = {} # propname : class
    references_col_classes = {}

    for name in attrs.keys():
      if isinstance(attrs[name], LinkedDocuments):
        links[name] = prop = attrs.pop(name)
        if prop.collection_name:
          if prop.collection_name in prop.reference_class._meta["_links"] or prop.collection_name in prop.reference_class._meta["_references"]:
            raise RiakkitError("%s already in %s!" % (prop.collection_name, prop.reference_class))
          l = linked_docs_col_classes.get(prop.collection_name, [])
          l.append(prop.reference_class)
          linked_docs_col_classes[prop.collection_name] = l

      elif isinstance(attrs[name], ReferenceBaseProperty):
        references[name] = prop = attrs.pop(name)
        if prop.collection_name:
          if prop.collection_name in prop.reference_class._meta["_links"] or prop.collection_name in prop.reference_class._meta["_references"]:
            raise RiakkitError("%s already in %s!" % (prop.collection_name, prop.reference_class))
          l = references_col_classes.get(prop.collection_name, [])
          l.append(prop.reference_class)
          references_col_classes[prop.collection_name] = l

      elif isinstance(attrs[name], BaseProperty):
        meta[name] = prop = attrs.pop(name)
        if prop.unique:
          prop.unique_bucket = attrs["client"].bucket(
              getUniqueListBucketName(clsname, name)
          )
          uniques.append(name)

        propDefaultValue = prop.defaultValue()
        if propDefaultValue is not None:
          hasdefaults[name] = propDefaultValue

    # reversed because we start at the bottom.

    all_parents = reversed(walkParents(parents))
    for p_cls in all_parents:
      p_meta = copy(p_cls._meta) # Shallow copy should be ok.
      p_links = p_meta.pop("_links")
      p_references = p_meta.pop("_references")
      meta.update(p_meta)
      links.update(p_links)
      references.update(p_references)
      hasdefaults.update(p_cls._hasdefaults)
      uniques.extend(p_cls._uniques)

    meta["_links"] = links
    meta["_references"] = references
    attrs["_meta"] = meta
    attrs["_uniques"] = uniques
    attrs["_hasdefaults"] = hasdefaults
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

    for col_name, reference_classes in linked_docs_col_classes.iteritems():
      for rcls in reference_classes:
        rcls._meta["_links"][col_name] = LinkedDocuments(reference_class=new_class)
    for col_name, reference_classes in references_col_classes.iteritems():
      for rcls in reference_classes:
        rcls._meta["_references"][col_name] = MultiReferenceProperty(reference_class=new_class)

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

    self._obj = self.bucket.get(self.key) if saved else None
    self._links = {}
    self._data = {}

    self.mergeData(kwargs)
    self._saved = saved

    self.__class__.instances[self.key] = self


  def mergeData(self, data):
    """Merges data. This will trigger the standard processors.

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
    keys = getKeys(data, cls._meta, cls._meta["_references"])
    for k in keys:
      if k in cls._meta:
        if k in data:
          data[k] = cls._meta[k].convertFromDb(data[k])
        else:
          data[k] = cls._meta[k].defaultValue()
      elif k in cls._meta["_references"]:
        if k in data:
          data[k] = cls._meta["_references"][k].convertFromDb(data[k])
        else:
          data[k] = cls._meta["_references"][k].defaultValue()

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

    if isinstance(riak_obj, (str, unicode)):
      riak_obj = cls.bucket.get(riak_obj)

    if not riak_obj.exists():
      raise NotFoundError("%s not found!" % riak_obj.get_key())

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
    links = riak_obj.get_links()
    obj._links = obj.updateLinks(links)
    obj._obj = riak_obj
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
        validator = lambda x: True
        standardizer = lambda x: x
        if name in self._meta:
          validator = self._meta[name].validate
          standardizer = self._meta[name].standardize
        elif name in self._meta["_references"]:
          validator = self._meta["_references"][name].validate
          standardizer = self._meta["_references"][name].standardize

        if not validator(value):
          raise ValueError("Validation did not pass for %s for the field %s.%s" % (value, self.__class__.__name__, name))
        value = standardizer(value)
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

    # Process regular data
    keys = getKeys(self._meta, self._data)

    for name in keys:
      if name not in self._meta:
        data_to_be_saved[name] = self._data[name]
        continue

      if name not in self._data:
        if self._meta[name].required:
          raise AttributeError("'%s' is required for '%s'." % (name, self.__class__.__name__))
        self._data[name] = self._meta[name].defaultValue()
      else:
        if self._meta[name].unique:
          if self._meta[name].unique_bucket.get(self._meta[name].convertToDb(self._data[name])).exists() and not self.__class__.bucket.get(self.key).exists():
            raise ValueError("'%s' already exists for '%s'!" % (self._data[name], name))

      data_to_be_saved[name] = self._meta[name].convertToDb(self._data[name])

    other_docs_to_be_saved = []

    # Process References
    for name in self._meta["_references"]:
      if name not in self._data:
        if self._meta["_references"][name].required:
          raise AttributeError("'%s' is required for '%s'." % (name, self.__class__.__name__))
        self._data[name] = self._meta["_references"][name].defaultValue()
      else:
        col_name = self._meta["_references"][name].collection_name

        if col_name:
          if isinstance(self._meta["_references"][name], ReferenceProperty):
            docs = [self._data[name]]
          else:
            docs = self._data[name]
          for doc in docs:
            current_list = doc._data.get(col_name, [])
            key_list = [d.key for d in current_list]
            if self.key not in key_list:
              current_list.append(self)
              doc._data[col_name] = current_list
              other_docs_to_be_saved.append(doc)

      data_to_be_saved[name] = self._meta["_references"][name].convertToDb(self._data[name])

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
          if doc._obj is None:
            raise RiakkitError("Add link failure as %s does not exist in the database." % str(doc))

          if col_name:
            current_list = doc._links.get(col_name, [])
            if self.key not in [d.key for d in current_list]:
              current_list.append(self)
              doc._links[col_name] = current_list
              other_docs_to_be_saved.append(doc)


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
      data = self._cleanupDataFromDatabase(deepcopy(self._obj.get_data()))
      self._data = data
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

      for name in self._uniques:
        obj = self._meta[name].unique_bucket.get(self._data[name])
        obj.delete()

      self._obj = None
      self._saved = False


  @classmethod
  def exists(cls, key, r=None):
    """Check if a key exists.

    Args:
      key: The key to check if exists or not.
      r: The R value

    Returns:
      True if the key exists, false otherwise.
    """
    riak_obj = cls.bucket.get(key, r)
    return riak_obj.exists()

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
