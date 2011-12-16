from setuptools import setup
import riakkit

setup(
  name = "riakkit",
  version = riakkit.VERSION,
  author = "Shuhao Wu",
  author_email = "shuhao@shuhaowu.com",
  description = ("An object mapper for Riak similar to mongokit, couchdbkit and"
                 " riakalchemy for Riak"),
  license = "GPLv3",
  keywords = "riak object mapper riakkit database",
  url = "https://github.com/ultimatebuster/riakkit",
  packages = ["riakkit"],
  classifiers = [
    "Development Status :: 3 - Alpha",
    "License :: OSI Approved :: GNU General Public License (GPL)",
    "Intended Audience :: Developers",
    "Topic :: Database",
    "Topic :: Software Development :: Libraries :: Python Modules"
  ]
)
