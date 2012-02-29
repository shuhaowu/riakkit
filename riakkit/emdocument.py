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

from riakkit.exceptions import *
from riakkit.types import BaseProperty, ReferenceProperty, MultiReferenceProperty, ReferenceBaseProperty
from riakkit.utils import *


class EmDocumentMetaclass(type):
  """Meta class that the EmDocuments class is made from."""
  def __new__(cls, clsname, parents, attrs):
    if clsname == "EmDocument": # TODO: inheritance..
      return type.__new__(cls, clsname, parents, attrs)

    meta = {}

    for name in attrs.keys():
      if isinstance(attrs[name], BaseProperty):
        meta[name] = prop = attrs.pop(name)

    all_parents = reversed(walkParents(parents, ("EmDocument", "object", "type")))
    for p_cls in all_parents:
      meta.update(copy(p_cls._meta))

    attrs["_meta"] = meta
    return type.__new__(cls, clsname, parents, attrs)


class EmDocument(dict):
  """The base Embedded Document class for embedded documents to extend from.

  EmDocument (Embedded document) is a Document like class that could be embedded
  into documents. This extends both from the dict type.

  EmDocument should be able to embed EmDocument as well. When saved to the
  database, EmDocument becomes a dictionary.

  EmDocument is not similar to Document and it has a lot of limitations. First
  off, there is no unique checking, there is not LinkedDocuments and for
  ReferenceBaseProperty, there's no collection_name AND there's no linking to
  other EmDocument. EmDocument doesn't have all the APIs of Document. It's more
  like a fancy DictProperty with type conversion and checking. No keys, either.
  """

  __metaclass__ = EmDocumentMetaclass

  def __init__(self, x=None, **kwargs):
    """Initialize a new EmDocumentInstance. Same interface as the dict class.

    Note: .update hasn't been implemented yet to be EmDocument safe.

    Args:
      x: A dictionary or None
      kwargs: Keyword arguments
    """
    dict.__init__(self)
    if x is not None:
      iteritems = x.iteritems()
    else:
      iteritems = kwargs.iteritems()

    for name, prop in self._meta.iteritems():
      dict.__setitem__(self, name, prop.defaultValue())

    for k, v in iteritems:
      self.__setitem__(k, v)

  def _error(self, name):
    raise AttributeError("Attribute %s not found with %s." %
        (name, self.__class__.__name__))

  @classmethod
  def cleanupDataFromDatabase(cls, data):
    """Cleans up the data from the database. Returns the same dictionary.

    Will modify the data dictionary.
    Used internally, you probably don't need this.

    Args:
      data: The data from the database as a dictionary

    Returns:
      The same data dictionary but altered.

    """
    keys = getKeys(data, cls._meta, discard_key=False)
    for k in keys:
      if k in cls._meta:
        if k in data:
          data[k] = cls._meta[k].convertFromDb(data[k])
        else:
          data[k] = cls._meta[k].defaultValue()

    return data


  def dbFriendlyData(self):
    """Checks the data and returns the db friendly data.

    This method will probably never be called manually, as it is used internally
    This method will modify the data! (Default values if data not present)

    Returns:
      A dictionary of data that's db friendly.

    """
    data_to_be_saved = {}
    keys = getKeys(self._meta, self, discard_key=False)
    for name in keys:
      if name not in self._meta:
        data_to_be_saved[name] = dict.__getitem__(self, name)
        continue

      if name not in self or dict.__getitem__(self, name) is None:
        if self._meta[name].required:
          raise AttributeError("'%s' is required for '%s'." % (name, self.__class__.__name__))
        dict.__setitem__(self, name, self._meta[name].defaultValue())

      data_to_be_saved[name] = self._meta[name].convertToDb(dict.__getitem__(self, name)) # Faster because self.__getitem__ does checks

    return data_to_be_saved

  def __getitem__(self, name):
    inMeta = name in self._meta
    inData = name in self

    if inData:
      return dict.__getitem__(self, name)

    if inMeta and not inData:
      return None

    if not inMeta and not inData:
      self._error(name)

  def __setitem__(self, name, value):
    validator = lambda x: True
    standardizer = lambda x: x
    if name in self._meta:
      validator = self._meta[name].validate
      standardizer = self._meta[name].standardize

    if not validator(value):
      raise ValueError("Validation did not pass for %s for the field %s.%s" % (value, self.__class__.__name__, name))
    value = standardizer(value)
    dict.__setitem__(self, name, value)

  __getattr__ = __getitem__
  __setattr__ = __setitem__
  __delattr__ = dict.__delitem__
