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

import unittest
import random

from riakkit.simple import *
from riakkit.commons.properties import *
from riakkit.commons.exceptions import *

def integerkeys(d):
  if d is None:
    return None

  nd = {}
  for key, value in d.iteritems():
    nd[int(key)] = value
  return nd

class TestModel(BaseDocument):
  booleanprop = BooleanProperty()
  floatprop = FloatProperty(required=True)
  intprop = IntegerProperty(default=lambda: random.randint(10, 20))
  listprop = ListProperty(validators=lambda x: len(x) >= 2)
  stringprop = StringProperty(standardprocessors=lambda x: None if x is None else x.strip().lower())
  dictprop = DictProperty(backwardprocessors=integerkeys)
  datetimeprop = DateTimeProperty()
  floatprocessorprop = FloatProperty(forwardprocessors=lambda x: None if x is None else x + 1)

class SimpleTestModel(SimpleDocument):
  someprop = StringProperty()

class ReferenceTestModel(BaseDocument):
  ref = ReferenceProperty(reference_class=SimpleTestModel)
  multirefs = MultiReferenceProperty(reference_class=SimpleTestModel)
  refsdict = DictReferenceProperty(reference_class=SimpleTestModel)

class RiakkitSimpleTests(unittest.TestCase):
  def setUp(self):
    self.testobj = TestModel()
    self.simpleobj = SimpleTestModel()

  def test_simpleObj(self):
    self.assertTrue(hasattr(self.simpleobj, "key"))
    self.assertTrue(isinstance(self.simpleobj.key, basestring))
    self.assertRaises(NotImplementedError, self.simpleobj.save)

  def test_emptySerialization(self):
    self.simpleobj.someprop = "lol"
    self.simpleobj.clear()
    self.assertEqual({"someprop" : None}, self.simpleobj.serialize())

  def test_getValues(self):
    testobj = self.testobj
    testobj.clear()

    self.assertTrue(10 <= testobj.intprop <= 20)
    self.assertEqual(None, testobj.booleanprop)
    self.assertEqual(None, testobj.stringprop)
    self.assertRaises(AttributeError, lambda: testobj.notaproperty)

  def test_setValuesSimple(self):
    testobj = self.testobj
    testobj.clear()

    # Check for validation errors
    self.assertFalse(testobj.valid())
    self.assertRaises(ValidationError, testobj.serialize)

    testobj.floatprop = 1.0
    self.assertEqual(1.0, testobj.floatprop)

    validationErrored = False
    try:
      testobj.listprop = []
    except ValidationError:
      validationErrored = True

    self.assertTrue(validationErrored)

    self.assertFalse(testobj.valid())
    self.assertRaises(ValidationError, testobj.serialize)

    testobj.listprop.append(2)
    testobj.listprop.append(3)

    self.assertTrue(testobj.valid())
    dictionary = testobj.serialize()

    self.assertEqual(1.0, dictionary[u"floatprop"])
    self.assertTrue(10 <= dictionary[u"intprop"] <= 20)
    self.assertEqual([2, 3], dictionary[u"listprop"])
    self.assertEqual(None, dictionary[u"booleanprop"])

  def test_processors(self):
    testobj = self.testobj
    testobj.clear()

    testobj.stringprop = "  Hello   "
    self.assertEqual(5, len(testobj.stringprop))
    self.assertEqual("hello", testobj.stringprop)

    testobj.floatprop = 2.0 # To avoid validation error for required
    testobj.floatprocessorprop = 1.0

    # remember that listprop is initialized using defaultValue, and via clear()
    # clear() doesn't perform validation checks.

    self.assertRaises(ValidationError, testobj.serialize)

    # listprop is already initialized to []. We can directly append to it.
    # __getattr__ returns a reference

    testobj.listprop.append(2)
    testobj.listprop.append(3)

    dictionary = testobj.serialize()
    self.assertEqual(2.0, dictionary["floatprocessorprop"])

    testobj.deserialize({"dictprop" : {"1" : 2}})
    self.assertEqual(None, testobj.dictprop.get("1", None))
    self.assertEqual(2, testobj.dictprop.get(1, None))

  def test_references(self):
    refobj = ReferenceTestModel()
    refobj.ref = self.simpleobj
    self.simpleobj.clear()
    self.simpleobj.someprop = "moo"
    dictionary = refobj.serialize()
    self.assertEqual(dictionary["ref"], self.simpleobj.key)


if __name__ == "__main__":
  unittest.main()
