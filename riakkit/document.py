# -*- coding: utf-8 -*-
# This file is part of Riakkit
#
# Riakkit is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Riakkit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with Riakkit. If not, see <http://www.gnu.org/licenses/>.

from __future__ import absolute_import
try:
  import simplejson as json
except ImportError:
  import json

from .properties.standard import BaseProperty
from .helpers import walk_parents
from .riakbridge import FakeRiakObject
from .exceptions import ValidationError

class DocumentMetaclass(type):
  def __new__(cls, clsname, parents, attrs):
    if clsname in ("Document", "EmDocument"):
      return type.__new__(cls, clsname, parents, attrs)

    meta = {}
    for name in attrs.keys():
      if isinstance(attrs[name], BaseProperty):
        meta[name] = attrs.pop(name)

    all_parents = reversed(walk_parents(parents))

    for p_cls in all_parents:
      meta.update(p_cls._meta)

    attrs["_meta"] = meta
    return type.__new__(cls, clsname, parents, attrs)

  def __getattr__(self, name):
    if hasattr(self, "_meta") and name in self._meta:
      return self._meta[name]
    raise AttributeError("'{0}' does not exist for class '{1}'.".format(name, self.__name__))


class EmDocument(object):
  """Embedded document as a JSON object"""
  __metaclass__ = DocumentMetaclass

  def __init__(self, data={}):
    self.clear()
    self.merge(data)

  def _validation_error(self, name, value):
    raise ValidationError("'{0}' doesn't pass validation for property '{1}'".format(value, name))

  def _attribute_not_found(self, name):
    raise AttributeError("Attribute '{0}' not found with '{1}'.".format(name, self.__class__.__name__))

  def serialize(self, dictionary=True):
    d = {}
    for name, value in self._data.iteritems():
      if name in self._meta and isinstance(self._meta[name], BaseProperty):
        if not self._meta[name].validate(value):
          self._validation_error(name, value)
        value = self._meta[name].to_db(value)

      d[name] = value

    if dictionary:
      return d
    else:
      return json.dumps(d)

  @classmethod
  def deserialize(cls, data):
    converted_data = {}
    props_to_load = set()

    for name, value in data.iteritems():
      if name in cls._meta:
        if cls._meta[name].load_on_demand:
          props_to_load.add(name)
        else:
          value = cls._meta[name].from_db(value)

      converted_data[name] = value

    doc = cls()
    doc.merge(converted_data, True)
    doc._props_to_load = props_to_load
    return doc

  def is_valid(self):
    for name in self._meta:
      if not self._validate_attribute(name):
        return False
    return True

  def invalids(self):
    invalid = []
    for name in self._meta:
      if not self._validate_attribute(name):
        invalid.append(name)

    return invalid

  def _validate_attribute(self, name):
    if name not in self._data:
      self._attribute_not_found(name)

    if name in self._meta:
      return self._meta[name].validate(self._data[name])

    return True

  def merge(self, data, merge_none=False):
    if isinstance(data, EmDocument):
      data = data._data
    elif isinstance(data, basestring):
      data = json.loads(data)

    for name, value in data.iteritems():
      if not merge_none and name in self._meta and value is None:
        continue
      self.__setattr__(name, value)

    return self

  def clear(self, to_default=True):
    self._data = {}
    self._props_to_load = set()

    if to_default:
      for name, prop in self._meta.iteritems():
        self._data[name] = prop.default()
    else:
      for name, prop in self._meta.iteritems():
        self._data[name] = None

    return self

  def __setattr__(self, name, value):
    if name[0] == "_":
      self.__dict__[name] = value
      return

    if name in self._meta:
      if hasattr(self._meta[name], "on_set"):
        value = self._meta[name].on_set(value)

    self._data[name] = value

  def __getattr__(self, name):
    if name in self._data:
      if name in self._props_to_load:
        self._data[name] = self._meta[name].from_db(self._data[name])
        self._prop_to_load.discard(name)
      return self._data[name]
    self._attribute_not_found(name)

  def __delattr__(self, name):
    if name in self._data:
      if name in self._meta:
        self._data[name] = None
      else:
        print name
        del self._data[name]
    else:
      self._attribute_not_found(name)

  __setitem__ = __setattr__
  __getitem__ = __getattr__
  __delitem__ = __delattr__

class Document(EmDocument, FakeRiakObject):
  __metaclass__ = DocumentMetaclass
