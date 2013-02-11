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

from ..exceptions import DocumentNotFoundError

_NOUNCE = object()

class BaseProperty(object):
  """Base property type
    All property types are required to be extended from this class.
  """

  def __init__(self, required=False, default=_NOUNCE, validators=[], load_on_demand=False):
    """Initializes a new instance of a property.

    Args:
      required: True or false, this will test for requiredness (None value will
      not fly!) Defaults to False
      default: a default value or a function that takes no argument that
               generates a default value. Defaults to None.
               Note: if you use this as a variable, passing an instance into
               it maybe hurtful as there's no deepcopying going on. If you want
               a default value of {}, do default=lambda: {}
      validators: A list of functions that takes in a value to validate. This
                  function must return True or False.
                  Note: if you want to turn off validations, set this to None.
      load_on_demand: A boolean value indicating if we want to convert the value
      upon getting from the database or when it is first accessed.
    """
    self.required = required
    self._default = default
    self._validators = validators
    if self._validators == None:
      self._validators = lambda v: True

    self.load_on_demand = load_on_demand

  def validate(self, value):
    if value is None:
      return not self.required

    if callable(self._validators):
      return self._validators(value)
    elif isinstance(self._validators, (list, tuple)):
      for validator in self._validators:
        if not validator(value):
          return False

    return True

  def to_db(self, value):
    """Converts a value to a database friendly format. Called upon saving to the
    database.
    Args:
      value: the value to be converted

    Returns:
      Whatever that type wants to convert. Default does nothing. If you're
      implementing this it needs to be JSON friendly.
    """
    return value

  def from_db(self, value):
    """Converts a value from the database back into something we work with.
    Called upon getting from the database. If self.load_on_demand is set to
    True, this method will be called when first accessed instead of on db.

    Args:
      value: Value from the database

    Returns:
      Whatever that type wants to be once its in application code rather than
      db.
    """
    return value

  def default(self):
    """Returns the default value of the property. It will return either the
    default value given (or generate one via the function) or the default for
    that type"""
    if callable(self._default):
      return self._default()

    return None if self._default is _NOUNCE else self._default

# standard properties... boring stuff
# This are strict, if you want to relax, use Property instead.

class StringProperty(BaseProperty):
  """Simple string property. Values will be converted to unicode."""
  def to_db(self, value):
    return None if value is None else unicode(value)

class NumberProperty(BaseProperty):
  """NumberProperty. Encompasses integer and floats.
  This always converts to floating points.
  No complex numbers here!"""
  def validate(self, value):
    if not BaseProperty.validate(self, value):
      return False

    if value is None:
      return True

    try:
      float(value)
    except (TypeError, ValueError):
      return False
    else:
      return True

  def to_db(self, value):
    return None if value is None else float(value)

class BooleanProperty(BaseProperty):
  """Boolean property. Values will be converted to boolean upon save."""
  def to_db(self, value):
    return None if value is None else bool(value)

class DictProperty(BaseProperty):
  """Dictionary property. Value must be an instance of a dictionary (or subclass)."""
  def __init__(self, **args):
    BaseProperty.__init__(self, **args)
    if self._default is _NOUNCE:
      self._default = lambda: {}

  def validate(self, value):
    return BaseProperty.validate(self, value) and (value is None or isinstance(value, dict))

class ListProperty(BaseProperty):
  """List property. Value must be an instance of a list/tuple (or subclass)."""
  def __init__(self, **args):
    BaseProperty.__init__(self, **args)
    if self._default is _NOUNCE:
      self._default = lambda: []

  def validate(self, value):
    return BaseProperty.validate(self, value) and (value is None or isinstance(value, (tuple, list)))

class EmDocumentProperty(BaseProperty):
  """Embedded document property. Value must be an embedded document or
  a dictionary"""
  def __init__(self, emdocument_class, **args):
    """Initializes a new embedded document property.

    Args:
      emdocument_class: The EmDocument child class.
      Everything else are inheritted from BaseProperty
    """
    BaseProperty.__init__(self, **args)
    self.emdocument_class = emdocument_class

  def validate(self, value):
    return BaseProperty.validate(self, value) and \
           isinstance(value, (self.emdocument_class, dict))

  def to_db(self, value):
    return None if value is None else value.serialize()

  def from_db(self, value):
    return None if value is None else self.emdocument_class.deserialize(value)

class EmDocumentsListProperty(BaseProperty):
  """A list of embedded documents. Probably shouldn't abuse this."""
  def __init__(self, emdocument_class, **args):
    """Initializes a new embedded document property.

    Args:
      emdocument_class: The EmDocument child class.
      Everything else are inheritted from BaseProperty
    """
    BaseProperty.__init__(self, **args)
    self.emdocument_class = emdocument_class
    if self._default is _NOUNCE:
      self._default = lambda: []

  def validate(self, value):
    if not BaseProperty.validate(self, value):
      return False

    for d in value:
      if d is not None and not isinstance(d, (self.emdocument_class, dict)):
        return False

    return True

  def to_db(self, value):
    return [None if d is None else d.serialize() for d in value]

  def from_db(self, value):
    return [None if d is None else self.emdocument_class.deserialize(d) for d in value]

class ReferenceProperty(BaseProperty):
  """Reference property. Stores the key of the other class in the db and
  retrieves on demand. Probably shouldn't even use this as 2i is better in
  most scenarios."""

  def __init__(self, reference_class, strict=False, **kwargs):
    BaseProperty.__init__(self, **kwargs)
    self.reference_class = reference_class
    self.strict = strict

  def validate(self, value):
    return BaseProperty.validate(self, value) and \
           isinstance(value, self.reference_class)

  def to_db(self, value):
    if value is None or isinstance(value, basestring):
      return value

    return value.key

  def from_db(self, value):
    doc = self.reference_class.deserialize(value)
    if doc is None and self.strict:
      raise DocumentNotFoundError("Document '{0}' for '{1}' not found and "
                                  "strict mode is active!".format(
                                      value,
                                      self.reference_class.__name__
                                  ))