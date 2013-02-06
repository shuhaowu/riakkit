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

# TODO: objects will be gone in py3k? Investigate
def walk_parents(parents, bases=("Document", "EmDocument", "type", "object")):
  """Walks through the parents and return each parent class object uptil the
  name of the classes specified in `bases`. Implemented using Wikipedia's BFS
  pseudocode.

  Args:
    p: The list of direct parents of a class object
    bases: The name of the classes that's considered to be the ones that should
           not be included and also terminates when encountered. defaults to
           ("Document", "type", "object")

  Returns:
    A list of all the parents ordered via BFS.
  """

  frontier = parents
  next_clses = []
  all_parents = []
  found = False
  while not found:
    found = True
    for cls in frontier:
      if cls.__name__ not in bases:
        found = False
        if cls not in all_parents:
          all_parents.append(cls)
          # cls.__bases__ are all the parent classes directly above cls
          next_clses.extend(cls.__bases__)
    frontier = next_clses
    next_clses = []
  return all_parents

def mediocre_copy(obj):
  """It's kind of like a deep copy, but it only make copies of lists, tuples,
  and dictionaries (and other primitive types). Other complex object such as
  ones you created are kept as references.

  Arg:
    obj: Any object.

  Returns:
    A meh copy of the obj as described.
  """

  if isinstance(obj, list):
    return [mediocre_copy(i) for i in obj]
  if isinstance(obj, tuple):
    return tuple(mediocre_copy(i) for i in obj)
  if isinstance(obj, dict):
    return dict(mediocre_copy(i) for i in obj.iteritems())

  return obj

# This needs to be compatible with RiakLink for this whole system to work.
# Compatibility note (this may be obsolete.. but if I'm developing this it won't be):
#   Required Attributes:
#     - key
#     - bucket
#     - tag = bucket if tag is None else tag
# Also provides the same convenience method of get, which gets the link as a
# riakkit document.
class Link(object):
  def __init__(self, bucket, key, tag=None):
    self.bucket = bucket
    self.key = key
    self.tag = bucket if tag is None else tag

  def get(self, r=None):
    raise NotImplementedError