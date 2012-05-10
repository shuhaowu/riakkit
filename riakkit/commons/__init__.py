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

uuid1Key = lambda kwargs: uuid1().hex

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
