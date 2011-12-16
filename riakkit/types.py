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

import datetime
import time

class BaseProperty(object):
  """Base property type

  All property types are required to be extended from this class.

  Attributes:
    required: Enforce this property to be required. (Boolean)
    unique: Enforce this property to be unique. (Boolean)
    validators: A list of callables or 1 callable that validates any value
                given. The function should be callback(value), returning
                a boolean.
  """
  def __init__(self, required=False, unique=False, validators=False):
    """Initializes the property field

    Args:
      required: A boolean that determines if this is required or not.
      unique: A boolean that determines if this is unique or not.
      validators: A list of callables of 1 callable that validates any value.
                  The function should be callback(value), returning a boolean.
    """
    self.required = required
    self.unique = unique
    self.validators = validators

  def convert(self, value):
    """Converts the value from any form to an usable form.

    In theory, the value should always be valid (as it has to undergo validate
    before this is called).

    Args:
      value: The value to be converted
    """
    return value

  def convertBack(self, value):
    """Converts the value from the database back to the original mapped value.

    Args:
      value: The value to be converted
    """
    return value

  def validate(self, value):
    """The default validation function.

    This validation function goes through the validators and checks each one.
    Any subclass should do:

    return BaseProperty.validate(self, value) and your_result

    where your_result is your validation result.

    Args:
      value: The value to be validated

    Returns:
      True if validation pass, False otherwise.
    """

    if callable(self.validators):
      return self.validators(value)
    elif type(self.validators) in (tuple, list):
      for validator in self.validators:
        if not validator(value):
          return False

    return True

  def defaultValue(self):
    """The default value for this type

    Returns:
      The default value for this type.
    """
    return None

class DictProperty(BaseProperty):
  """Dictionary property, {}"""

  def defaultValue(self):
    """Default value for dictionary

    Returns:
      {}
    """
    return {}

class ListProperty(BaseProperty):
  """List property, []"""
  def defaultValue(self):
    """Default value for list

    Returns:
      []
    """
    return []

class StringProperty(BaseProperty):
  """String property. By default this converts strings to unicode."""
  def convert(self, value):
    return unicode(value)

class IntegerProperty(BaseProperty):
  """Integer property."""
  def convert(self, value):
    return int(value)

  def defaultValue(self):
    """Default value for integer

    Returns:
      0
    """
    return 0

  def validate(self, value):
    try:
      int(value)
    except ValueError:
      return False
    return BaseProperty.validate(self, value) and True

class FloatProperty(BaseProperty):
  """Floating point property"""
  def convert(self, value):
    return float(value)

  def defaultValue(self):
    """Default value for integer

    Returns:
      0.0
    """
    return 0.0

  def validate(self, value):
    try:
      float(value)
    except:  # POKEMON EXCEPTION! :D
      return False
    return BaseProperty.validate(self, value) and True

class BooleanProperty(BaseProperty):
  """Boolean property. Pretty self explanatory."""
  def convert(self, value):
    return bool(value)

class EnumProperty(BaseProperty):
  """Not sure if enum is the best name, but this only allows some properties.

  This also only stores integers into database, making it efficient.
  The values you supply it initially will be the ones you get back (references
  will be kept if they are objects, so try to use basic types).
  """

  def __init__(self, possible_values, required=False,
                     unique=False, validators=False):
    """Initialize the Enum Property.

    Args:
      possible_values: Possible values to be taken.

    Everything else is inheritted from BaseProperty.
    Note: Probably a bad idea using unique on this, but no one is preventing you
    """
    BaseProperty.__init__(self, required, unique, validators)
    self._map_forward = {}
    self._map_backward = {}
    for i, v in enumerate(possible_values):
      self._map_forward[v] = i
      self._map_backward[i] = v

  def validate(self, value):
    return BaseProperty.validate(self, value) and (value in self._map_forward)

  def convert(self, value):
    return self._map_forward.get(value)

  def convertBack(self, value):
    return self._map_backward.get(value)

class DateTimeProperty(BaseProperty):
  """The datetime property.

  Stores the UTC time as a float in the database. Maps to the python object of
  datetime.datetime or the utc timestamp.
  """

  def validate(self, value):
    check = False
    if isinstance(value, (long, int, float)): # timestamp
      try:
        value = datetime.datetime.utcfromtimestamp(value)
      except ValueError:
        check = False
      else:
        check = True
    elif isinstance(value, datetime.datetime):
      check = True
    return BaseProperty.validate(self, value) and check

  def convert(self, value):
    if isinstance(value, (long, int, float)): # timestamp, validation has passed
      return value
    return time.mktime(value.utctimetuple())

  def convertBack(self, value):
    """Converts the value from the database back to a datetime object.

    Args:
      value: The value to be converted

    Returns:
      The corresponding datetime object that's based off utc.
    """
    return datetime.datetime.utcfromtimestamp(value)

  def defaultValue(self):
    return datetime.datetime.utcnow()


class DynamicProperty(BaseProperty):
  """Allows any type of property... Probably a bad idea, but sometimes useful.

  The implementation of this is exactly identical as BaseProperty. It doesn't
  do anything at all.. (except using your own validators).

  Warning: You may experience problems if your types doesn't play nice with
  Python json module...
  """

class LinkedDocuments(BaseProperty):
  """Linked documents property.

  This is always a list and they reference other documents. This property
  disallow uniqueness. Setting .unique will have no effect.

  Keep in mind that when you call .save on your object,
  it will change the objects that you're linking to as well, as the
  implementation will call save there.
  """
  def __init__(self, reference_class=None, collection_name=None,
                     required=False, unique=False, validators=False):
    """Initializes a LinkedDocuments property.

    You can set it up so that riakkit automatically link back from
    the reference_class like GAE's ReferenceProperty. Except everything here
    is a list.

    Args:
      reference_class: The classes that should be added to this LinkedDocuments.
                       i.e. Documents have to be objects of that class to pass
                       validation. None if you wish to allow any Document class.
      collection_name: The collection name for the reference_class. Works the
                       same way as GAE's collection_name for their
                       ReferenceProperty. See the README file at the repository
                       for detailed tutorial.
    """
    BaseProperty.__init__(self, required=required,
        unique=unique, validators=validators)

    self.reference_class = reference_class

    if collection_name and reference_class:
      self.collection_name = collection_name
    else:
      self.collection_name = None

  def _checkForReferenceClass(self, l):
    rc = self.reference_class or Document

    for v in l:
      if not isinstance(v, rc):
        return False
    return True

  def validate(self, value):
    check = self._checkForReferenceClass(value)
    return BaseProperty.validate(self, value) and check

  def defaultValue(self):
    return []
