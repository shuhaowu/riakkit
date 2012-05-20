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

from riakkit import *
from riakkit.helpers import emailValidator, checkPassword
from riakkit.commons import getUniqueListGivenClassName

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

class SimpleReferenceModel(SimpleDocument):
  ref = ReferenceProperty(SimpleModel)


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
    obj = SimpleModel(intprop=5, someprop="lol")
    obj2 = SimpleModel()
    obj.addLink(obj2, None)
    obj.addIndex("field_bin", "testvalue")

    c = riak.RiakClient()
    b = c.bucket("test")
    ro = obj.toRiakObject(b)

    self.assertEquals(ro.get_key(), obj.key)
    self.assertEquals({u"listprop": [], u"booleanprop": True, u"intprop": 5, u"someprop": u"lol"}, ro.get_data())
    self.assertEquals(1, len(ro.get_indexes()))
    self.assertEquals(["testvalue"], ro.get_indexes("field_bin"))
    self.assertEquals(1, len(ro.get_links()))
    self.assertEquals(obj2.key, ro.get_links()[0].get_key())

  def test_fromRiakObject(self):
    c = riak.RiakClient()
    b = c.bucket("test")
    o = b.new("o1")
    o.set_data({"intprop" : 1})
    o.add_index("field_bin", "str")
    doc = SimpleModel.load(o)
    self.assertEquals(1, doc.intprop)
    self.assertEquals(1, len(doc.indexes()))
    self.assertEquals({"str"}, doc.index("field_bin"))

  def test_simpleReferences(self):
    c = riak.RiakClient()
    b = c.bucket("test")
    doc = SimpleModel()
    refdoc = SimpleReferenceModel()
    refdoc.ref = doc
    o = refdoc.toRiakObject(b)
    self.assertEquals(doc.key, o.get_data()[u"ref"])

    refdoc = SimpleReferenceModel.load(o)
    self.assertEquals(doc.key, refdoc.ref)

###############################################################################
###############################################################################
###############################################################################

class BaseDocumentModel(Document):
  client = riak.RiakClient()

class User(BaseDocumentModel):
  bucket_name = "test_users"

  username = StringProperty(unique=True, required=True)
  password = PasswordProperty(required=True)
  email = StringProperty(validators=emailValidator, unique=True)

class Comment(BaseDocumentModel):
  bucket_name = "test_comments"

  author = ReferenceProperty(User, collection_name="comments")
  content = StringProperty()

class RiakkitDocumentTests(unittest.TestCase):
  def _getRidOfPreviousUniqueUsername(self, username):
    c = riak.RiakClient()
    ub = c.bucket(getUniqueListGivenClassName("User", "username"))
    uo = ub.get(username)
    uo.delete()

  def test_save(self):

    user1 = User(username="foo_save", password="123")
    user1.someprop = 1
    user1.save()

    c = riak.RiakClient()
    b = c.bucket("test_users")
    o = b.get(user1.key)
    self.assertTrue(o.exists())
    d = o.get_data()
    self.assertEquals(user1.username, d["username"])
    self.assertEquals(user1.password.hash, d["password"]["hash"])
    self.assertEquals(user1.password.salt, d["password"]["salt"])
    self.assertEquals(user1.someprop, d["someprop"])
    self.assertEquals(None, d["email"]) # Should be none
    self.assertEquals(user1.email, d["email"])

    o.delete()

    ub = c.bucket(getUniqueListGivenClassName("User", "username"))
    uo = ub.get("foo_save")
    self.assertTrue(uo.exists())
    uo.delete()

  def test_cachingDelete(self):
    user1 = User(username="foo", password="123")
    key = user1.key
    self.assertTrue(key in User.instances)
    del user1
    self.assertFalse(key in User.instances)

  def test_reload(self):
    user1 = User(username="foo_reload", password="123")
    user1.save()
    key = user1.key

    # Modify data
    c = riak.RiakClient()
    b = c.bucket("test_users")
    o = b.get(key)
    d = o.get_data()
    d["lol"] = "moo"
    d["email"] = "test@test.com"
    o.set_data(d)
    o.store()

    user1.reload()
    self.assertEquals("foo_reload", user1.username)
    self.assertEquals(key, user1.key)
    self.assertEquals("moo", user1.lol)
    self.assertEquals("test@test.com", user1.email)

    user1.delete()

  def test_load(self):
    user1 = User(username="foo_load", password="123")
    user1.save()
    key = user1.key

    user1Copy = User.load(key)
    self.assertTrue(user1 is user1Copy)
    del user1
    del user1Copy
    user1 = User.load(key)
    self.assertEquals("foo_load", user1.username)
    self.assertTrue(checkPassword("123", user1.password))
    self.assertEquals(None, user1.email)
    user1.delete()

  def test_2i(self):
    user1 = User(username="foo_2i", password="123")
    user1.addIndex("field_bin", "lol")
    user1.save()
    key = user1.key
    del user1
    user1 = User.load(key)
    self.assertEquals({"lol"}, user1.indexes("field_bin"))

    q = User.indexLookup("field_bin", "lol")
    self.assertEquals(1, q.length())
    for u in q.run():
      self.assertEquals(key, u.key)
      self.assertEquals("foo_2i", u.username)
      self.assertEquals({"lol"}, user1.indexes("field_bin"))

    user1.delete()

  def test_exists(self):
    user1 = User(username="foo_exists", password="123")
    user1.save()
    self.assertTrue(User.exists(user1.key))
    user1.delete()

  def test_referencesSimple(self):
    user1 = User(username="foo_refsimp", password="123")
    comment1 = Comment(author=user1, content="Hello World!")
    comment1.save() # should save user1. user1.save won't save comment1 because comment1 is not finalized yet and it is the origin

    self.assertTrue(comment1 in user1.comments)
    user1key = user1.key
    comment1key = comment1.key

    del user1
    del comment1

    user1 = User.load(user1key)
    self.assertEquals(1, len(user1.comments))
    self.assertEquals(comment1key, user1.comments[0].key)
    self.assertEquals(user1, user1.comments[0].author)
    self.assertEquals("Hello World!", user1.comments[0].content)

    comment2 = Comment(author=user1, content="Hello World 2!")
    comment2.save()
    user1.reload()

    self.assertEquals(2, len(user1.comments))
    user1.comments.sort(key=lambda x: x.content)
    self.assertEquals(comment2.key, user1.comments[0].key)
    self.assertEquals(comment1key, user1.comments[1].key)

    user1.comments[0].delete()
    user1.comments[0].delete()
    user1.delete()

  def test_referencesDeleteTarget(self): # deletes user, as Comment is the origin
    user1 = User(username="refdeltarget", password="123")
    comment1 = Comment(author=user1, content="Hello World!")
    comment1.save()

    user1.delete()
    self.assertEquals(None, comment1.author)
    comment1.reload()
    self.assertEquals(None, comment1.author)
    comment1.delete()

  def test_referencesDeleteOrigin(self): # deletes the comment and watches the response of the user1
    user1 = User(username="refdelorigin", password="123")
    comment1 = Comment(author=user1, content="Hello World!")
    comment1.save()

    user1.reload()
    self.assertEquals(1, len(user1.comments))
    comment1.delete()
    self.assertEquals(0, len(user1.comments))
    user1.reload()
    self.assertEquals(0, len(user1.comments))
    user1.delete()

  def test_uniques(self):
    user1 = User()
    user1.username = "foo"
    user1.password = "123"
    user1.save()

    user2 = User(username="foo", password="123")
    self.assertRaises(ValueError, user2.save)

    self.assertTrue(User._meta["username"].unique_bucket.get("foo").exists())
    user1.delete()
    c = riak.RiakClient()
    self.assertFalse(User._meta["username"].unique_bucket.get("foo").exists())

  def test_passwordProperty(self):
    user = User()
    def t():
      user.password = 123

    self.assertRaises(TypeError, t)
    user.password = "123456"
    self.assertTrue(checkPassword("123456", user.password))
    hsh = user.password.hash
    user.password = "123456"
    self.assertNotEquals(hsh, user.password.hash)

if __name__ == "__main__":
  base = unittest.TestSuite()
  base.addTest(unittest.makeSuite(RiakkitBaseTest))

  simple = unittest.TestSuite()
  simple.addTest(unittest.makeSuite(RiakkitSimpleTest))

  document = unittest.TestSuite()
  document.addTest(unittest.makeSuite(RiakkitDocumentTests))

  alltests = unittest.TestSuite([base, simple, document])

  import sys
  suite = eval(sys.argv[1]) if len(sys.argv) > 1 else alltests
  unittest.TextTestRunner(verbosity=2).run(suite)
