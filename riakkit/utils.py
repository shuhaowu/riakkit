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

"""This module provides some utility function for both internal and external
uses."""

def walkParents(parents):
  """Walks through the parents and return each parent class object uptil
  Document class or object. The Document or object class will not be returned.

  Args:
    p - The list of direct parents of a class object

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
      if cls.__name__ not in ("Document", "type", "object"):
        found = False
        if cls not in all_parents: # Inefficient
          all_parents.append(cls)
          next.extend(cls.__bases__)

    frontier = next
    next = []

  return all_parents

if __name__ == "__main__":
  class A(object): pass
  class B(A): pass
  class C(B): pass
  class D(A): pass
  class E(D, C): pass
  print "Parent walk passed?",
  print str(walkParents(E.__bases__)) == "[<class '__main__.D'>, <class '__main__.C'>, <class '__main__.A'>, <class '__main__.B'>]"
