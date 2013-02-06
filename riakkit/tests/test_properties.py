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

import unittest
from ..properties.standard import (
  BaseProperty,
  StringProperty,
  NumberProperty,
  BooleanProperty,
  DictProperty,
  ListProperty
)

class StandardPropertiesTest(unittest.TestCase):
  def test_default(self):
    """Test case for default value"""
    prop = BaseProperty()
    self.assertEquals(None, prop.default())
    prop = BaseProperty(default="yay")
    self.assertEquals("yay", prop.default())
    x = 1
    prop = BaseProperty(default=lambda: x + 1)
    self.assertEquals(2, prop.default())

  def test_validators(self):
    """Test case for validators"""
    prop = BaseProperty()
    self.assertTrue(prop.validate("wut"))
    prop = BaseProperty(validators=lambda x: x == 1)
    self.assertTrue(prop.validate(1))
    self.assertFalse(prop.validate(2))
    self.assertTrue(prop.validate(None)) # None values are okay unless required = True
    prop = BaseProperty(validators=[lambda x: x % 2 == 0, lambda x: x % 3 == 0])
    self.assertTrue(prop.validate(6))
    self.assertFalse(prop.validate(2))
    self.assertFalse(prop.validate(3))
    self.assertTrue(prop.validate(None))

  def test_required(self):
    """Test case for required"""
    prop = BaseProperty(required=True)
    self.assertFalse(prop.validate(None))
    self.assertTrue(prop.validate("something"))

  def test_stringprop(self):
    """Test case for string property"""
    prop = StringProperty()

    self.assertTrue(prop.validate(1))
    self.assertEquals("1", prop.to_db(1))

    self.assertTrue(prop.validate(None))
    self.assertEquals(None, prop.to_db(None))

    self.assertTrue(prop.validate(False))
    self.assertEquals("False", prop.to_db(False))

    self.assertTrue(prop.validate("yay"))
    self.assertEquals("yay", prop.to_db("yay"))

  def test_numprop(self):
    """Test case for number property"""
    prop = NumberProperty()

    self.assertTrue(prop.validate(1))
    self.assertEquals(1.0, prop.to_db(1))

    self.assertTrue(prop.validate(2.5))
    self.assertEquals(2.5, prop.to_db(2.5))

    self.assertTrue(prop.validate("3.14"))
    self.assertEquals(3.14, prop.to_db(3.14))

    # Yup, gotta preprocess this yourself
    self.assertFalse(prop.validate("0x99"))

  def test_booleanprop(self):
    """Test case for boolean property"""
    prop = BooleanProperty()

    self.assertTrue(prop.validate(True))
    self.assertEquals(True, prop.to_db(True))

    self.assertTrue(prop.validate(False))
    self.assertEquals(False, prop.to_db(False))

    self.assertTrue(prop.validate(1))
    self.assertEquals(True, prop.to_db(1))

    self.assertTrue(prop.validate(None))
    self.assertEquals(None, prop.to_db(None))

  def test_dictproperty(self):
    """Test case for dictionary property"""
    prop = DictProperty()

    d1 = prop.default()
    self.assertEquals({}, d1)
    d2 = prop.default()
    self.assertFalse(d1 is d2)

    self.assertTrue(prop.validate({"yay" : []}))
    self.assertEquals({"yay" : []}, prop.to_db({"yay" : []}))

    self.assertTrue(prop.validate({}))
    self.assertEquals({}, prop.to_db({}))

    self.assertTrue(prop.validate(None))
    self.assertEquals(None, prop.to_db(None))

    self.assertFalse(prop.validate("a string"))
    self.assertFalse(prop.validate(123))
    self.assertFalse(prop.validate(False))

  def test_listproperty(self):
    """Test case for list property"""
    prop = ListProperty()

    l1 = prop.default()
    l2 = prop.default()
    self.assertEquals([], l1)
    self.assertFalse(l1 is l2)

    self.assertTrue(prop.validate([]))
    self.assertEquals([], prop.to_db([]))

    self.assertTrue(prop.validate(tuple()))

    # We don't care because tuples will convert to lists via json library
    self.assertEquals(tuple(), prop.to_db(tuple()))

    self.assertTrue(prop.validate(None))
    self.assertEquals(None, prop.to_db(None))

    self.assertTrue(prop.validate([1, 2, 3]))
    self.assertEquals([1, 2, 3], prop.to_db([1, 2, 3]))

    self.assertTrue(prop.validate((1, 2, 3)))
    self.assertEquals((1, 2, 3), prop.to_db((1, 2, 3)))

  def test_emdocumentprop(self):
    pass

  def test_emdocumentslistprop(self):
    pass

  def test_referenceprop(self):
    pass