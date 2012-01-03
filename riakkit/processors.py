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
# This file contains some common processors that's useful for app development.

import hashlib

def hashPassword(password, salt, algorithm=hashlib.sha1):
  """Create a password hash string with a password and a salt. Default sha1

  Arguments:
    password: The plain text password to be hashed
    salt: A salt to be added to the password. This is required.
    algorithm: The hashlib algorithm. Default: hashlib.sha1
  Return:
    Returns the hashed password.
  Note:
    You NEED to create your own function as forward and backwardprocessors
    only take 1 argument.
    So yourfunction = lambda v: hashPassword(v, SALT)
  """
  return algorithm(password + salt).hexdigest()

