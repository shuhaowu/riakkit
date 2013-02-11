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

import unittest

from ..document import EmDocument
from ..properties import *
from ..exceptions import ValidationError

class SimpleDocument(EmDocument):
  s = StringProperty()
  i = NumberProperty()
  l = ListProperty()
  sr = StringProperty(required=True)
  sv = StringProperty(validators=lambda v: v == "valid")
  sd = StringProperty(default="default")

class EmDocumentTest(unittest.TestCase):
  def test_property_initialization(self):
    doc = SimpleDocument()
    self.assertEquals(None, doc.s)
    self.assertEquals(None, doc.i)
    self.assertEquals([], doc.l)
    self.assertEquals(None, doc.sr)
    self.assertEquals(None, doc.sv)
    self.assertEquals("default", doc.sd)
    with self.assertRaises(AttributeError):
      doc.not_an_attribute

  def test_initialization_with_data(self):
    doc = SimpleDocument({"s" : "test", "l": [1]})
    self.assertEquals(None, doc.i)
    self.assertEquals("test", doc.s)
    self.assertEquals([1], doc.l)

  def test_data_modification(self):
    doc = SimpleDocument()
    doc.i = 5
    doc.s = "meow"
    doc.l.append(1)
    self.assertEquals(5, doc.i)
    self.assertEquals("meow", doc.s)
    self.assertEquals([1], doc.l)

    del doc.i
    self.assertEquals(None, doc.i)

    doc.prop = "yay"
    self.assertEquals("yay", doc.prop)

    del doc.prop
    with self.assertRaises(AttributeError):
      doc.prop

  def test_validate(self):
    doc = SimpleDocument()
    self.assertFalse(doc.is_valid())

    # Note that here is sr only because sv is valid as it doesn't explicitly
    # say that it is required. None simply means non-existent.
    self.assertEquals(["sr"], doc.invalids())

    doc.sr = "required"
    doc.sv = "valid"
    self.assertTrue(doc.is_valid())
    self.assertEquals([], doc.invalids())

    doc.sv = "invalid"
    self.assertFalse(doc.is_valid())
    del doc.sr
    self.assertEquals(["sr", "sv"], sorted(doc.invalids()))

  def test_merge(self):
    doc = SimpleDocument()

    doc.i = 5

    doc.merge({"sr" : "overwrite1", "sv": "overwrite1"})
    self.assertEquals("overwrite1", doc.sr)
    self.assertEquals("overwrite1", doc.sv)
    self.assertEquals(5, doc.i)

    doc2 = SimpleDocument()
    doc2.i = 6
    doc2.sr = "required"
    doc.merge(doc2)
    self.assertEquals(6, doc.i)
    self.assertEquals("required", doc.sr)
    self.assertEquals("overwrite1", doc.sv)

  def test_clear(self):
    doc = SimpleDocument()
    doc.i = 5
    doc.s = "yay"
    self.assertEquals(5, doc.i)
    self.assertEquals("yay", doc.s)

    del doc.sd
    # We test the clear to default first
    doc.clear()
    self.assertEquals(None, doc.i)
    self.assertEquals(None, doc.s)
    self.assertEquals("default", doc.sd)

    doc.clear(False)

    self.assertEquals(None, doc.sd)

  def test_serialize(self):
    doc = SimpleDocument()
    with self.assertRaises(ValidationError):
      doc.serialize()

    doc.i = 5
    doc.sr = "required"
    doc.sv = "valid"

    expected = {"i": 5, "sr": "required", "sv": "valid", "sd": "default", "l": [], "s": None}
    self.assertEquals(expected, doc.serialize())

    doc.prop = "test"
    expected = {"i": 5, "sr": "required", "sv": "valid", "sd": "default", "l": [], "s": None, "prop": "test"}
    self.assertEquals(expected, doc.serialize())

  def test_deserialize(self):
    doc = SimpleDocument.deserialize({"i": 5, "sr": "required", "sv": "valid", "l": [1]})
    self.assertEquals(5, doc.i)
    self.assertEquals("required", doc.sr)
    self.assertEquals("valid", doc.sv)
    self.assertEquals("default", doc.sd)
    self.assertEquals([1], doc.l)
    self.assertEquals(None, doc.s)

    doc = SimpleDocument.deserialize({"i": 5, "sr": "required", "l": [1], "sd": None, "prop": "test"})
    self.assertEquals(None, doc.sd)
    self.assertEquals(None, doc.sv)
    self.assertEquals("test", doc.prop)

if __name__ == "__main__":
  unittest.main()