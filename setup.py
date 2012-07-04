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
import os

from setuptools import setup, find_packages

here = os.path.abspath(os.path.dirname(__file__))


def _get_meta_var(name, data, callback_handler=None):
    import re
    matches = re.compile(r'(?:%s)\s*=\s*(.*)' % name).search(data)
    if matches:
        if not callable(callback_handler):
            callback_handler = lambda v: v

        return callback_handler(eval(matches.groups()[0]))

_meta = open(os.path.join(here, 'riakkit', '__init__.py'), 'rb')
_metadata = _meta.read()
_meta.close()

callback = lambda V: ('.'.join(map(str, V[:3])) + '.'.join(V[3:]))
__version__ = _get_meta_var('VERSION', _metadata, callback)
__author__ = _get_meta_var('__author__', _metadata)
__url__ = _get_meta_var('__url__', _metadata)

setup(
    name="riakkit",
    version=__version__,
    author=__author__,
    author_email="shuhao@shuhaowu.com",
    description=("An object mapper for Riak similar to mongokit and couchdbkit"
                 " for Riak"),
    license="LGPL",
    keywords="riak object mapper riakkit database orm",
    url=__url__,
    packages=find_packages(),
    install_requires=['riak'],
    classifiers=[
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: GNU Library or Lesser General Public License (LGPL)",
        "Intended Audience :: Developers",
        "Topic :: Database",
        "Topic :: Software Development :: Libraries :: Python Modules"
    ]
)
