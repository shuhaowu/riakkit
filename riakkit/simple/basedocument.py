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

from riakkit.commons import walkParents, uuid1Key
from riakkit.commons.properties import BaseProperty, ReferenceBaseProperty
from riakkit.commons.exceptions import ValidationError

from copy import copy, deepcopy
import json
from riak.mapreduce import RiakLink

class BaseDocumentMetaclass(type):
  def __new__(cls, clsname, parents, attrs):
    if clsname in ("BaseDocument", "SimpleDocument"):
      return type.__new__(cls, clsname, parents, attrs)

    meta = {}
    for name in attrs.keys():
      if isinstance(attrs[name], BaseProperty):
        meta[name] = attrs.pop(name)

    all_parents = reversed(walkParents(parents, ("BaseDocument", "SimpleDocument", "object", "type")))

    for p_cls in all_parents:
      meta.update(copy(p_cls._meta))
    attrs["_meta"] = meta

    return type.__new__(cls, clsname, parents, attrs)

  def __getattr__(self, name):
    if name in self._meta:
      return self._meta[name]
    raise AttributeError

DEFAULT_VALIDATOR = lambda x: True
DEFAULT_CONVERTER = lambda x: x

class BaseDocument(object):
  """The BaseDocument class is the lowest level of abstraction ther is. This
  is essentially what dictshield has, probably even simpler (having never used
  dictshield in the first place).

  It's recommended that this is not used as SimpleDocument and Document is
  prefered.

  EmDocument is currently defined as EmDocument = BaseDocument. However, this
  behaviour may change in the future.
  """
  __metaclass__ = BaseDocumentMetaclass

  # This is not a real object. Objects created with this class will not have key
  # Either embedded document or otherwise.
  # TODO: Make this better. Potentially using some sort of detection system.
  # Or make a subclass of this that will be used as people abandon the overhead
  # of the RAD and use the core for efficiency.
  _clsType = 0

  def __init__(self, **kwargs):
    """Initialize a new BaseDocument.

    Args:
      kwargs: The keyword arguments to merge into the object.
    """
    self.clear()

    # TODO: combine with mergeData
    keys = set(self._meta.keys())
    for name, value in kwargs.iteritems():
      self.__setattr__(name, value)
      keys.discard(name)

    for name in keys:
      self._data[name] = self._meta[name].defaultValue()

  def _attrError(self, name):
    raise AttributeError("Attribute %s not found with %s." %
        (name, self.__class__.__name__))

  def _valiError(self, value, name):
    raise ValidationError("%s doesn't pass validation for property '%s'" % (value, name))

  def serialize(self, dictionary=True):
    """Serializes the data to database friendly version.

    This *only* returns the dictionary of values.

    Args:
      dictionary: If True, this function will return a dictionary passed back
                  to riak-python-client. Otherwise it will return a JSON.
    Returns:
      A dictionary or a string. Depending on the value of dictionary.
    """
    d = {}
    for name, value in self._data.iteritems():
      self._processOneValue(d, name, value)

    if dictionary:
      return d
    else:
      return json.dumps(d)

  def _processOneValue(self, d, name, value):
    prop = self._meta.get(name, None)
    converter = DEFAULT_CONVERTER
    if prop is not None:
      converter = prop.convertToDb

    if not self.validate(name):
      self._valiError(value, name)

    value = converter(value)

    d[unicode(name)] = value

  def valid(self):
    """Validate all the values.

    Returns:
      True if validation passes, False otherwise
    """
    for name, prop in self._meta.iteritems():
      if not self.validate(name):
        return False

    return True

  def validate(self, name):
    """Validate a specific property.

    Args:
      name: The name of the property. If it's not defined in the schema, this
            method will always return True

    Returns:
      True if valid, False otherwise.
    """
    if name in self._meta:
      prop = self._meta[name]
      value = self._data[name]
      if prop.required and value is None:
        return False
      else:
        return prop.validate(value)
    return True

  @classmethod
  def constructObject(cls, data):
    """Construct an object given some data.

    This is basically deserialize but as a classmethod.
    Args:
      data: Same thing as deserialize
      dictionary: Same thing as deserialize
    Returns:
      The object constructed with this class
    """
    return cls().deserialize(data)

  def deserialize(self, data):
    """Deserializes some data into the document.

    With this function, we assume the data is from the database, therefore we
    call convertFromDb. This method will also clear the document.

    Args:
      data: The data, either a dictionary or a json string.

    Returns:
      self for OOP purposes.
    """
    if isinstance(data, basestring):
      data = json.loads(data)

    self.clear()
    keys = set(self._meta.keys())
    for name, value in data.iteritems():
      prop = self._meta.get(name, None)
      if prop is not None:
        converter = prop.convertFromDb
      else:
        converter = DEFAULT_CONVERTER

      value = converter(value)
      self._data[name] = value
      keys.discard(name)

    for name in keys:
      self._data[name] = self._meta[name].defaultValue()

    return self

  def mergeData(self, data):
    """Merges some data into the document.

    With this function, we assume the data is from just the input source,
    therefore we call standardize. This method will NOT clear the document.

    Args:
      data: The data, either a dictionary of a json string.

    Returns:
      self for OOP"""

    if isinstance(data, basestring):
      data = json.loads(data)

    for name, value in data.iteritems():
      self.__setattr__(name, value)

    return self


  def clear(self, setdefault=True):
    """Clears the document, clears all the data stored.

    Returns:
      self for OOP"""
    self._data = {}

    if setdefault:
      for name, prop in self._meta.iteritems():
        self._data[name] = prop.defaultValue()

    return self

  def __setattr__(self, name, value):
    if name.startswith("_"):
      self.__dict__[name] = value
      return

    validator = lambda x: True
    standardizer = lambda x: x
    if name in self._meta:
      validator = self._meta[name].validate
      standardizer = self._meta[name].standardize

    if not validator(value):
      raise ValidationError("Validation did not pass for %s for the field %s.%s" % (value, self.__class__.__name__, name))
    value = standardizer(value)
    self._data[name] = value

  def __getattr__(self, name):
    if name in self._data:
      prop = self._meta.get(name, BaseProperty)
      if isinstance(prop, ReferenceBaseProperty):
        self._data[name] = prop.attemptLoad(self._data[name])
      return self._data[name]

    self._attrError(name)

  def __delattr__(self, name):
    if name in self._data:
      if name in self._meta:
        self._data[name] = None
      else:
        del self._data[name]
    else:
      raise KeyError("'%s'" % name)

  __setitem__ = __setattr__
  __getitem__ = __getattr__
  __delitem__ = __delattr__


class SimpleDocument(BaseDocument):
  """This is a low level abstract of how objects that would be directly saved
  into Riak (ones associated with a key, not an embedded document).

  This class provides nearly all operations that Document provides, except
  the ones that's interacts directly with Riak.

  unique no longer has any meanings here as it's SimpleDocument's job to enforce
  this rule. That's your problem.
  """
  _clsType = 1

  def __init__(self, key=uuid1Key, **kwargs):
    """Creates a SimpleDocument object.

    Args:
      key: A key string or a callable. Defaults to generating uuid1
      **kwargs: Any data that you want to be merged into the document
    """
    if callable(key):
      key = key(kwargs)

    self.__dict__["key"] = key

    BaseDocument.__init__(self, **kwargs)

  def clear(self, setdefault=True):
    self._indexes = {}
    self._links = set()
    return BaseDocument.clear(self, setdefault)

  def save(self, **kwargs):
    """Not available in SimpleDocument.

    raises:
      NotImplementedError
    """
    raise NotImplementedError("Remember the good old day when we played with Documents? Well as an adult now, you don't save SimpleDocuments anymore.")

  reload = save
  delete = save

  def addIndex(self, field, value):
    """Adds an index field : value

    Args:
      field: The field name. Remember that fieldnames have to end with the
             correct types, and currently it's _bin and _int.
      value: The value for this field.

    Returns:
      self for OOP purposes.
    """
    l = self._indexes.get(field, set())
    l.add(value)
    self._indexes[field] = l
    return self

  def removeIndex(self, field, value=None):
    """Removes an index field : value

    Args:
      field: The field name.
      value: The value, defaults to None. If value is none, the whole field gets
             removed, otherwise only that specific pair will get removed.
    Returns:
      self for OOP purposes
    """
    if value is None:
      self._indexes.pop(field)
    else:
      if field in self._indexes:
        self._indexes[field].discard(value)
        if len(self._indexes[field]) == 0:
          self._indexes.pop(field)

    return self

  def setIndexes(self, indexes):
    """Sets the indexes. Overwrites the previous indexes

    Args:
      indexes: Format should be {"fieldname" : {"fieldvalue"}, "fieldname2" : {"fieldvalue"}}.
               A deep copy is made here.

    Returns:
      self for OOP purposes.
    """
    self._indexes = deepcopy(indexes)
    return self

  def indexes(self, field=None):
    """Retrives the whole index or a specific list of indexes for a field.

    Args:
      field: the field name. Defaults to None. If it is None, the all the indexes will be returned.

    Returns:
      The set of field values or a list of (field, value) pairs friendly for set_indexes
    """
    if field is not None:
      return copy(self._indexes[field])

    i = []
    for field, l in self._indexes.iteritems():
      for value in l:
        i.append((field, value))

    return i

  index = indexes # Done so that index(fieldname) is grammatically correct

  def addLink(self, document, tag=None):
    """Adds a link for the document.

    Args:
      document: A SimpleDocument object or its child. Checking will not be done.
      tag: A tag. Defaults to None

    Returns:
      self for OOP purposes"""
    self._links.add((document, tag))
    return self

  def removeLink(self, document, tag=None):
    """Removes a link from the document

    Args:
      document: A SimpleDocument object or its child.
      tag: A tag value.

    Returns:
      self for OOP purposes"""
    l = set()
    for d, t in self._links:
      if d.key != document.key or tag != t: # TODO: Best way to do this?
        l.add((d, t))
    self._links = l
    return self

  def setLinks(self, links):
    """Sets the links. Overwrites the current links collection.

    Args:
      links: Format should be set((document, tag), (document, tag)).
             A shallow copy is made here.

    Returns:
      self for OOP purposes"""
    self._links = copy(links)
    return self

  def links(self, bucket=None):
    """Gets all the links.

    Args:
      bucket: Defaults to None. If it is a RiakBucket, this will return a list of RiakLinks instead of (document, tag) in a set

    Returns:
      A set of (document, tag) or [RiakLink, RiakLink]"""
    if bucket is not None:
      return [RiakLink(bucket.get_name(), d.key, t) for d, t in self._links]
    return copy(self._links)

  def toRiakObject(self, bucket):
    """Converts the SimpleDocument into a RiakObject. Does not touch references,
    unlike Document.save(). Nor does this actually save anything.

    Args:
      bucket: A RiakBucket object

    Returns:
      A RiakObject with data, indexes, and links set according to this
      SimpleDocument
    """
    obj = bucket.new(self.key, self.serialize())
    obj.set_indexes(self.indexes())
    obj.set_links(self.links(bucket), True)
    return obj

  @staticmethod
  def _getIndexesFromRiakObj(robj):
    objIndexes = robj.get_indexes()
    indexes = {}
    for indexEntry in objIndexes:
      field = indexEntry.get_field()
      value = indexEntry.get_value()
      l = indexes.get(field, set())
      l.add(value)
      indexes[field] = l
    return indexes

  @classmethod
  def load(cls, robj, cached=False):
    """Construct a SimpleDocument from a RiakObject. Similar to Document.load,
    but doesn't support links, nor does it load from the database via a key
    instead of a robj, nor does it do caching.

    Note that this function does not get the links as SimpleDocument can't do
    this due to technical reason (one way only).

    Args:
      robj: A RiakObject

    Returns:
      A SimpleDocument
    """
    doc = cls(robj.get_key())
    doc.deserialize(robj.get_data())
    doc.setIndexes(cls._getIndexesFromRiakObj(robj))
    return doc
