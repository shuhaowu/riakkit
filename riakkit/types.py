# RiakKit
# Copyright (C) 2011 Shuhao Wu
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

#You should have received a copy of the GNU General Public License
#along with this program.  If not, see <http://www.gnu.org/licenses/>.

class BaseProperty(object):
  """Base property type

  All property types are required to be extended from this class.

  Attributes:
    required: Enforce this property to be required. (Boolean)
    unique: Enforce this property to be unique (THIS HAS NOT BEEN IMPLEMENTED!)
            (Boolean)
    validators: A list of callables or 1 callable that validates any value
                given. The function should be callback(value), returning
                a boolean.
  """
  def __init__(self, required=False, unique=False, validators=False):
    """Initializes the property field

    Args:
      required: A boolean that determines if this is required or not.
      unique: A boolean that determines if this is unique or not. This feature
              has not yet been implemented
      validators: A list of callables of 1 callable that validates any value.
                  The function should be callback(value), returning a boolean.
    """
    self.required = required
    self.unique = unique  # TODO: unique validator
    self.validators = validators

  def convert(self, value):
    """Converts the value from any form to an usable form.

    In theory, the value should always be valid (as it has to undergo validate
    before this is called).

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
    except: # POKEMON EXCEPTION!
      return False
    return BaseProperty.validate(self, value) and True

class LinkedDocuments(BaseProperty):
  """Linked documents property.

  This is always a list and they reference other documents.
  """
  def defaultValue(self):
    return []

# TODO: DateTimeProperty ...
