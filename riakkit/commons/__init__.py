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

from uuid import uuid1
import os


uuid1Key = lambda kwargs: uuid1().hex
uuid1Key.__doc__ = """
Generates an UUID1 key. Used for Document and SimpleDocument. The arg kwargs
has no effect, only to be compatible with SimpleDocument and Document. This
is the default option."""

# TODO: switch default methods to generate keys.
_p = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
def rndstr(n):
  """Generates a random string from a-zA-Z0-9 using os.urandom.

  Could be a way to generate the keys. May collide more, not certain though.

  Args:
    n: The length of the string.
  Returns:
    a n-length string of random characters
"""
  t = ""
  while n > 0:
    i = ord(os.urandom(1))
    while i >= 248:
      i = ord(os.urandom(1))
    i %= 62
    t += _p[i]
    n -= 1
  return t


def getUniqueListGivenBucketName(bucketName, propertyName):
  """Gets the bucket name that enforces the uniqueness of a certain property.

  Args:
    bucketName: The name of the bucket of the class
    propertyName: The property name

  Returns:
    Returns the bucket name.
  """
  return "_%s_ul_%s" % (bucketName, propertyName)

def walkParents(parents, bases=("Document", "type", "object")):
  """Walks through the parents and return each parent class object uptil the
  name of the classes specified in bases.

  Args:
    p: The list of direct parents of a class object
    bases: The name of the classes that's considered to to be the ones that
           should not be included and the end nodes.
           Default: ("Document", "type", "object")

  Returns:
    A list of all the parents, every level. Ordered via breath first search,
    although each levels are not separated
  """
  frontier = parents
  next = []
  all_parents = []
  found = False
  while not found:
    found = True
    for cls in frontier:
      if cls.__name__ not in bases:
        found = False
        if cls not in all_parents: # Inefficient
          all_parents.append(cls)
          next.extend(cls.__bases__)

    frontier = next
    next = []

  return all_parents

def getProperty(name, attrs, parents):
  """Used in __new__ while getting attributes of class objects that's about to
  be created.

  This also looks up the ladder of the parents of this class object.

  Args:
    name: Name of the attribute
    attrs: The attributes from the __new__ methods.
    parents: The class objects that's the parent of this class from the __new__.

  Returns:
    None if the attributes is not found from the attrs nor the parents.
    Otherwise the first one that's found, from attrs to BFS'ed parents.
  """
  parents = walkParents(parents)
  value = attrs.get(name, None)
  i = 0
  length = len(parents)
  while value is None:
    if i == length:
      return None
    value = getattr(parents[i], name, None)
    i += 1

  return value

def getKeys(*args, **kwargs):
  """Gets the keys of all of the dictionaries and returns it in a list.

  Removing any references to 'key' (if specified)

  Args:
    discard_key: Boolean that determine whether or not to discard the 'key' attr
  """
  discard_key = kwargs.get("discard_key", True)
  keys = set()
  for d in args:
    keys.update(d.keys())

  if discard_key:
    keys.discard("key")

  return keys

def mediocreCopy(obj):
  """It's kind of like a deep copy, but it only make copies of lists, tuples,
  and dictionaries (and other primitive types). Other complex object such as
  ones you created are kept as references.

  Arg:
    obj: Any object.
  """
  if isinstance(obj, list): # TODO: Sets
    return [mediocreCopy(i) for i in obj]
  if isinstance(obj, tuple):
    return tuple(mediocreCopy(i) for i in obj)
  if isinstance(obj, dict):
    return dict(mediocreCopy(i) for i in obj.iteritems())
  return obj
