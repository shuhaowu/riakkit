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

from riakkit.commons import walkParents
from riakkit.commons.properties import BaseProperty
from riakkit.commons.exceptions import ValidationError

import json

class BaseDocumentMetaclass(type):
  def __new__(cls, clsname, parents, attrs):
    if clsname == "BaseDocument":
      return type.__new__(cls, clsname, parents, attrs)

    meta = {}
    for name in attrs.keys():
      if isinstance(attrs[name], BaseProperty):
        meta[name] = attrs.pop(name)

    all_parents = reversed(walkParents(parents, ("BaseDocument", "object", "type")))

    for p_cls in all_parents:
      meta.update(copy(p_cls._meta))
    attrs["_meta"] = meta

    return type.__new__(cls, clsname, parents, attrs)

DEFAULT_VALIDATOR = lambda x: True
DEFAULT_CONVERTER = lambda x: x

class BaseDocument(object):
  __metaclass__ = BaseDocumentMetaclass

  def __init__(self, **kwargs):
    self.mergeData(kwargs)

  def _attrError(self, name):
    raise AttributeError("Attribute %s not found with %s." %
        (name, self.__class__.__name__))

  def _valiError(self, value, name):
    raise ValidationError("%s doesn't pass validation for property '%s'" % (value, name))

  def serialize(self, dictionary=True):
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
    for name, prop in self._meta.iteritems():
      if not self.validate(name):
        return False

    return True

  def validate(self, name):
    if name in self._meta:
      prop = self._meta[name]
      value = self._data[name]
      if prop.required and value is None:
        return False
      else:
        return prop.validate(value)

  @classmethod
  def constructObject(cls, data, dictionary=True):
    return cls().deserialize(data, dictionary)

  def deserialize(self, data, dictionary=True):
    def action(name, value):
      prop = self._meta.get(name, None)
      validator = DEFAULT_VALIDATOR
      converter = DEFAULT_CONVERTER
      if prop is not None:
        validator = prop.validate
        converter = prop.convertFromDb

      if not validator(value):
        self._valiError(value, name)

      value = converter(value)
      self._data[name] = value

    return self._doMerge(action, data, dictionary)

  def mergeData(self, data, dictionary=True):
    def action(name, value):
      self.__setattr__(name, value)

    return self._doMerge(action, data, dictionary)

  def _doMerge(self, action, data, dictionary=True):
    if not dictionary:
      data = json.loads(data)

    self.clear()
    keys = set(self._meta.keys())
    for name, value in data.iteritems():
      action(name, value)
      keys.discard(name)

    for name in keys:
      self._data[name] = self._meta[name].defaultValue()

    return self

  def clear(self):
    self._data = {}
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

