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

# We need to make document compatible with RiakObject so we can put Document
# directly into client without going through an actual RiakObject. So in
# essence, the following 2 classes are for put only.

class FakeRiakBucket(object):
  """Something indistinguishable from RiakBucket to the transports

  Required attributes:
    - name: The name of the bucket

  Required methods:
    None
  """
  def __init__(self, name):
    self.name = name


class FakeRiakObject(object):
  """Something indistinguishable from RiakObject to the transports. This is
  the base class for Document. Should not be initialized on its own.

  Required attributes:
    - key: obviously the key
    - bucket: a RiakBucket or FakeRiakBucket
    - metadata: The metadata... in the format of riak. Keys are:
      - content-type: Always "application/json"
      - links: list of RiakLink (we are going to use our version because it is compatible)
      - index: list of (field, value)
      - usermeta: a dictionary of key -> value
      - charset: used in pbc only. Not needed?
      - content-encoding: used in pbc only. Not needed?
      - vtag: not needed
      - lastmod: not needed
      - lastmod-usec: not needed
      - deleted: not needed
    - usermeta: -> metadata["usermeta"]
    - vclock: just the vclock. Could be None, but must be something.
    - content_type: Always "application/json"

  Required methods:
    - get_encoded_data()
    - get_links() -> metadata["links"]
    - get_indexes() -> metadata["index"]

  This class setups the silly stuff (see source for __init__). Document will
  provide .key and .vclock. Document must call FakeRiakObject.__init__(self)
  after setting up! (FakeRiakObject expects self.bucket_name to be available)
  """

  def __init__(self):
    # Here we handle the silly stuffs.
    self.content_type = "application/json"
    self.metadata = {"links": [], "index": [], "usermeta": {}, "content-type": "application/json"}
    self.usermeta = self.metadata["usermeta"]
    self.bucket = FakeRiakBucket(self.bucket_name)
    self.get_encoded_data = self.serialize

  def get_links(self):
    """Do not call this function. It's for riak-python-client."""
    return self.metadata["links"]

  def get_indexes(self):
    """Do not call this function. It's for riak-python-client."""
    return self.metadata["index"]