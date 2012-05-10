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

from copy import copy, deepcopy
from weakref import WeakValueDictionary

from riakkit.simple.basedocument import BaseDocumentMetaclass, BaseDocument
from riakkit.commons.properties import BaseProperty
from riakkit.commons import uuid1Key

_document_classes = {}

def getClassGivenBucketName(bucket_name):
  """Gets the class associated with a bucket name.

  Args:
    bucket_name: The bucket name. String

  Returns:
    A document subclass associated with that bucket name

  Raises:
    KeyError if bucket_name is not used.
  """
  return _document_classes[bucket_name]

def getUniqueListBucketName(class_name, property_name):
  """Gets the bucket name that enforces the uniqueness of a certain property.

  Args:
    class_name: The name of the class
    property_name: The property name

  Returns:
    Returns the bucket name.
  """
  return "_%s_ul_%s" % (class_name, property_name)


class DocumentMetaclass(BaseDocumentMetaclass):
  """Meta class that the Document class is made from.

  Checks for bucket_name in each class, as those are necessary.
  """

  def __new__(cls, clsname, parents, attrs):
    if clsname == "Document":
      return type.__new__(cls, clsname, parents, attrs)

    client = getProperty("client", attrs, parents)
    if client is None:
      return type.__new__(cls, clsname, parents, attrs)

    meta = {}
    uniques = []
    references_col_classes = []
    references = []

    for name in attrs.keys():
      if isinstance(attrs[name], BaseProperty):
        meta[name] = prop = attrs.pop(name)
        colname = getattr(prop, "collection_name", False)
        if colname:
          if colname in prop.reference_class._meta:
            raise RiakkitError("%s already in %s!" % (colname, prop.reference_class))
          references_col_classes.append((colname, prop.reference_class, name))
          references.append(name)
        elif prop.unique: # Unique is not allowed with anything that has backref
          prop.unique_bucket = client.bucket(getUniqueListBucketName(clsname, name))
          uniques.append(name)

    # TODO: DUPLICATE WORK WITH getProperty
    all_parents = reversed(walkParents(parents))
    for p_cls in all_parents:
      meta.update(p_cls._meta)
      uniques.extend(p_cls._unique)

    attrs["_meta"] = meta
    attrs["_uniques"] = uniques
    attrs["instances"] = WeakValueDictionary()
    attrs["_references"] = references

    new_class = type.__new__(cls, clsname, parents, attrs)

    bucket_name = attrs.get("bucket_name", None)
    if bucket_name is not None:
      if bucket_name in _document_classes:
        raise RiakkitError("Bucket name of %s already exists in the registry!"
                              % new_class.bucket_name)
      else:
        _document_classes[bucket_name] = new_class

      new_class.bucket = client.bucket(bucket_name)

    for colname, rcls, back_name in references_col_classes:
      rcls._meta[colname] = MultiReferenceProperty(reference_class=new_class)
      rcls._meta[colname].is_reference_back = back_name

    return new_class

class Document(BaseDocument):
  """The base Document class for other classes to extend from.

  There are a couple of class variables that needs to be filled out. First is
  client. client is an instance of a RiakClient. The other is bucket_name. This
  is the name of the bucket to be stored in Riak. It must not be shared with
  another Document subclass. Lastly, you may set the SEARCHABLE to True or False

  Class variables that's an instance of the BaseType will be the schema of the
  document.
  """

  __metaclass__ = DocumentMetaclass
  _isRealObject = True

  def __init__(self, key=uuid1Key, saved=True, **kwargs):
    if callable(key):
      key = key(kwargs)

    if not isinstance(key, basestring):
      raise RiakkitError("%s is not a proper key!" % key)

    if key in self.__class__.instances:
      raise RiakkitError("%s already exists! Use get instead!" % key)

    self.__dict__["key"] = key

    self._obj = self.bucket.get(self.key) if saved else None
    self._links = set()
    self._indexes = {}
    self._saved = saved

    BaseDocument.__init__(self, **kwargs)

    self.__class__.instances[self.key] = self

  def save(self, w=None, dw=None):
    dataToBeSaved = self.serialize()
    uniquesToBeDeleted = []
    othersToBeSaved = []

    for name in self._uniques:
      if self._data.get(name, None) is None:
        if self._obj: # TODO: could be somehow refactored, as this condition is always true?
          originalValue = self._obj.get_data().get(name, None)
          if originalValue is not None:
            uniquesToBeDeleted.append((self._meta[name].unique_bucket, originalValue))
      else:
        changed = False
        if self._obj:
          originalValue = self._obj.get_data().get(name, None)
          if self._data[name] != originalValue and originalValue is not None:
            uniquesToBeDeleted.append(self._meta[name].unique_bucket, originalValue)
            changed = True
        else:
          changed = True

        if changed and self._meta[name].unique_bucket.get(dataToBeSaved[name]).exists():
          raise ValueError("'%s' already exists for '%s'!" % (self._data[name], name))

    for name in self._references:
      if self._obj:
        originalValues = self._obj.get_data().get(name, None)
        if originalValues is None:
          originalValues = []
        elif not isinstance(originalValues, list):
          originalValues = [originalValues]
      else:
        originalValues = []

      if isinstance(self._meta[name], ReferenceProperty):
        docs = [self._data[name]]
      else:
        docs = self._data[name]

      dockeys = set()
      colname = self._meta[name].collection_name

      for doc in docs: # These are foreign documents
        if doc is None:
          continue

        dockeys.add(doc.key)

        currentList = doc._data.get(colname, [])
        found = False # Linear search algorithm. Maybe binary search??
        for d in currentList:
          if d.key == self.key:
            found = True
            break
        if not found:
          currentList.append(self)
          doc._data[colname] = currentList
          othersToBeSaved.append(doc)

      for dockey in originals:
        if dockey is None:
          continue

        # This means that this specific document is not in the current version,
        # but last version. Hence it needs to be cleaned from the last version.
        if dockey not in dockeys:
          try:
            doc = self._meta[name].reference_class.load(dockey, True)
          except NotFoundError: # TODO: Another hackjob? This is _probably_ due to we're back deleting the reference.
            continue

          currentList = doc._data.get(colname, [])

          # TODO: REFACTOR WITH ABOVE'S LINEAR SEARCH ALGO
          for i, d in enumerate(currentList):
            if d.key == self.key:
              currentList.pop(i)
              doc._data[colname] = currentList
              othersToBeSaved.append(doc)
              break


    if self._obj:
      self._obj.set_data(dataToBeSaved)
    else:
      self._obj = bucket.new(self.key, dataToBeSaved)

    links = []
    for doc, tag in self._links:
      links.append(RiakLink(self.bucket_name, doc.key, tag))

    self._obj.set_links(links, True)

    indexes = []
    for field, l in self._index.iteritems():
      for value in l:
        indexes.append((field, value))

    self._obj.set_indexes(indexes)

    self._obj.store(w=w, dw=dw)
    for name in self._uniques:
      obj = self._meta[name].unique_bucket.new(self._data[name], {"key" : self.key})
      obj.store(w=w, dw=dw)

    for bucket, key in uniquesToBeDeleted:
      bucket.get(key).delete()

    self._saved = True

    for doc in othersToBeSaved:
      doc.save(w, dw)
