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
from datetime import datetime
import time

from .standard import BaseProperty

# Now, owl.
# ,___,  ,___,
# (O,O)  (O,O)
# /)_)    (_(\
#  ""      ""

# Less space than storing your whole string as we use ints.
# Probably will never use used.
# But why not.
class EnumProperty(BaseProperty):
  def __init__(self, possible_values, **args):
    BaseProperty.__init__(self, **args)

    self._map_forwards = {}
    for i, v in enumerate(possible_values):
      self._map_forwards[v] = i
    self._map_backwards = possible_values

  def validate(self, value):
    return BaseProperty.validate(self, value) and (value in self._map_forwards)

  def to_db(self, value):
    return None if value is None else self._map_forwards[value]

  def from_db(self, value):
    return None if value is None else self._map_backwards[int(value)]

# Hey something that may be used!
class DateTimeProperty(BaseProperty):

  def __init__(self, **args):
    BaseProperty.__init__(**args)
    if self._default is None:
      self._default = lambda: datetime.fromtimestamp(time.time())

  def validate(self, value):
    if not BaseProperty.validate(self, value):
      return False

    if isinstance(value, datetime):
      return True

    if isinstance(value, (long, int, float)): # timestamp
      try:
        datetime.fromtimestamp(value)
      except ValueError:
        return False
      else:
        return True

    return False

  def to_db(self, value):
    if value is None or isinstance(value, (long, int, float)):
      return value

    # We assume that value now have to be a datetime due to validate.
    return time.mktime(value.timetuple())

  def from_db(self, value):
    return None if value is None else datetime.fromtimestamp(value)


# Password stuffs... maybe used.. to make passwords not a hassle.
try:
  import bcrypt
except ImportError:
  import sys
  import os
  from hashlib import sha256

  print >> sys.stderr, "=================================================================="
  print >> sys.stderr, "WARNING: BCRYPT NOT AVAILABLE. DO NOT USE IN PRODUCTION WITHOUT IT"
  print >> sys.stderr, "=================================================================="

  # TODO: switch default methods to generate keys. Get someone else to figure
  # out security of the following..
  # In theory the developer will never use this.......
  _p = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
  def generate_salt():
    """Generates a random string from a-zA-Z0-9 using os.urandom.

  Could be a way to generate the keys. May collide more, not certain though.
  """
    n = 25 # 25 should be good?
    t = ""
    while n > 0:
      i = ord(os.urandom(1))
      while i >= 248:
        i = ord(os.urandom(1))
      i %= 62
      t += _p[i]
      n -= 1
    return t

  hash_password = lambda password, salt: sha256(password + salt).hexdigest()
else:
  generate_salt = lambda: bcrypt.gensalt()
  hash_password = lambda password, salt: bcrypt.hashpw(password, salt)

class PasswordProperty(BaseProperty):
  def on_set(self, value):
    if not isinstance(value, basestring):
      return TypeError("Password must be a basestring!")

    record = {}
    record["salt"] = generate_salt()
    record["hash"] = hash_password(value, record["salt"])
    return record

  @staticmethod
  def check_password(password, record):
    return hash_password(password, record["salt"]) == record["hash"]
