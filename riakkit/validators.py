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
#
# This file contains some common validators that's useful for app development.

"""This file contains some common validators that will be used in app
development for convinience purposes. Most of these can be plugged right into
the validators field for a Document.

Most of these functions are inline lambda funcitons.

"""

import re

_emailRegex = re.compile("[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*@(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?", flags=re.I)

# Credit goes to dictshield at https://github.com/j2labs/dictshield/blob/master/dictshield/fields/base.py
_urlRegex = re.compile(
    r'^https?://'
    r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'
    r'localhost|'
    r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'
    r'(?::\d+)?'
    r'(?:/?|[/?]\S+)$', re.IGNORECASE
  )

_regexMatch = lambda x, r: True if x is None else bool(r.match(x.strip().lower()))
emailValidator = lambda x: _regexMatch(x, _emailRegex) or x == ""
urlValidator = lambda x: _regexMatch(x, _urlRegex) or x == ""
