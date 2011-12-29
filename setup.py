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

from setuptools import setup
import riakkit

setup(
  name = "riakkit",
  version = riakkit.VERSION,
  author = "Shuhao Wu",
  author_email = "shuhao@shuhaowu.com",
  description = ("An object mapper for Riak similar to mongokit and couchdbkit"
                 " for Riak"),
  license = "LGPL",
  keywords = "riak object mapper riakkit database orm",
  url = "https://github.com/ultimatebuster/riakkit",
  packages = ["riakkit"],
  classifiers = [
    "Development Status :: 3 - Alpha",
    "License :: OSI Approved :: GNU Library or Lesser General Public License (LGPL)",
    "Intended Audience :: Developers",
    "Topic :: Database",
    "Topic :: Software Development :: Libraries :: Python Modules"
  ]
)
