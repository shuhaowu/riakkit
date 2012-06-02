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

"""This is the top level riakkit module. It provides a shortcut to the package
contents in a convinient fashion.

It imports everything from under commons.properties as well as commons.exceptions
It also import SimpleDocument, BaseDocument, and Document. This also sets up
EmDocument"""

from riakkit.simple import SimpleDocument, BaseDocument
EmDocument = BaseDocument
from riakkit.document import Document
from riakkit.commons.properties import *
from riakkit.commons.exceptions import *

VERSION = "0.6.0a"
