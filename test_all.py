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

import riak

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

class RiakkitBaseTest(unittest.TestCase):
  # Mainly to test the BaseDocument and Properties.
  # However, uses SimpleDocument to test some very basics.
  def setUp(self):
    self.testobj = TestModel()
    self.simpleobj = SimpleTestModel()

  def test_mergeDataFromInit(self):
    obj = TestModel(booleanprop=True)
    self.assertEquals(True, obj.booleanprop)
    self.assertTrue(10 <= obj.intprop <= 20)

    # Interesting case here. As what if the default value conflicts with the
    # validator (esp. the builtin defaults)
    self.assertRaises(ValidationError, lambda: TestModel(listprop=[]))

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
    # clear() doesn"t perform validation checks.

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

###############################################################################
###############################################################################
###############################################################################

class SimpleModel(SimpleDocument):
  intprop = IntegerProperty()
  listprop = ListProperty()
  booleanprop = BooleanProperty(default=True)

class RiakkitSimpleTest(unittest.TestCase):
  def test_keySetup(self):
    key1 = "somekey"
    obj = SimpleModel(key1)
    self.assertEquals(key1, obj.key)

    key2 = lambda kwargs: str(1 + 1)
    obj = SimpleModel(key2)
    self.assertEquals("2", obj.key)

  def test_simpleObj(self):
    obj = SimpleModel()
    self.assertTrue(hasattr(obj, "key"))
    self.assertTrue(isinstance(obj.key, basestring))
    self.assertRaises(NotImplementedError, obj.save)
    self.assertRaises(NotImplementedError, obj.reload)

  def test_objIndexes(self):
    obj = SimpleModel()
    self.assertEquals([], obj.indexes())
    self.assertEquals(obj, obj.addIndex("field_bin", "test")) # test OOP
    self.assertEquals([("field_bin", "test")], obj.indexes())
    obj.addIndex("field2_int", 42)
    self.assertEquals([("field2_int", 42), ("field_bin", "test")], sorted(obj.indexes()))

    self.assertEquals({42}, obj.index("field2_int"))
    self.assertEquals({"test"}, obj.index("field_bin"))

    obj.removeIndex("field2_int")
    self.assertEquals([("field_bin", "test")], obj.indexes())

    obj.addIndex("field_bin", "test2")
    self.assertEquals({"test", "test2"}, obj.indexes("field_bin"))
    obj.removeIndex("field_bin", "test")
    self.assertEquals({"test2"}, obj.index("field_bin"))
    obj.removeIndex("field_bin", "test2")

    self.assertRaises(KeyError, lambda: obj.index("field_bin"))

    obj.setIndexes({"field1_bin" : {"test1"}})
    self.assertEquals([("field1_bin", "test1")], obj.indexes())

  def test_objLinks(self):
    obj = SimpleModel("o1")
    obj2 = SimpleModel("o2")
    obj3 = SimpleModel("o3")

    self.assertEquals(set(), obj.links())

    obj.addLink(obj2)
    self.assertEquals({(obj2, None)}, obj.links())

    obj.addLink(obj3, "tag")
    self.assertEquals({(obj2, None), (obj3, "tag")}, obj.links())

    obj.addLink(obj3, "tag2")
    self.assertEquals({(obj2, None), (obj3, "tag"), (obj3, "tag2")}, obj.links())

    obj.removeLink(obj2)
    self.assertEquals({(obj3, "tag"), (obj3, "tag2")}, obj.links())

    obj.removeLink(obj3, "tag2")
    self.assertEquals({(obj3, "tag")}, obj.links())

    obj.removeLink(obj3, "tag")
    self.assertEquals(set(), obj.links())

    obj.setLinks({(obj2, "lol"), (obj3, None)})
    self.assertEquals({(obj2, "lol"), (obj3, None)}, obj.links())

    c = riak.RiakClient()
    b = c.bucket("test")

    links = sorted(obj.links(b), key=lambda x: x.get_key())
    self.assertEquals("o2", links[0].get_key())
    self.assertEquals("o3", links[1].get_key())

  def test_toRiakObject(self):
    obj = SimpleModel(intprop=5)
    obj2 = SimpleModel()
    obj.addLink(obj2, None)
    obj.addIndex("field_bin", "testvalue")

    c = riak.RiakClient()
    b = c.bucket("test")
    ro = obj.toRiakObject(b)

    self.assertEquals(ro.get_key(), obj.key)
    self.assertEquals({u"listprop": [], u"booleanprop": True, u"intprop": 5}, ro.get_data())
    self.assertEquals(1, len(ro.get_indexes()))
    self.assertEquals(["testvalue"], ro.get_indexes("field_bin"))
    self.assertEquals(1, len(ro.get_links()))
    self.assertEquals(obj2.key, ro.get_links()[0].get_key())

if __name__ == "__main__":
  unittest.main()
