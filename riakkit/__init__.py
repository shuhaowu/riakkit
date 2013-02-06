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

"""Top level riakkit module"""
from __future__ import absolute_import

# PEP 386 versioning
VERSION = (1, 0, 0, "a")
__version__ = ('.'.join(map(str, VERSION[:3])) + '.'.join(VERSION[3:]))
__author__ = "Shuhao Wu"
__url__ = "https://github.com/shuhaowu/riakkit"
