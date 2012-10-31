
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

class SolrQuery(object):
  """A wrapper around RiakSearch to play nice with Document and Solr

  Attributes:
    cls: The class for this SolrQuery
    result: The result dictionary.
  """
  def __init__(self, cls, result):
    self.cls = cls
    self.result = result
    self.loadDoc = lambda doc : self.cls.load(self.cls.bucket.get(doc[u"id"]))

  def length(self):
    """Gets the length of the documents that's searched through.

    Deprecated. Use len() instead."""
    return self.result[u"num_found"]

  __len__ = length

  def run(self):
    """Returns a generator that goes through each document that's searched."""
    for doc in self.result[u"docs"]:
      yield self.loadDoc(doc)

  def all(self):
    """Returns all the items that's found and return it.

    Return:
      A list of all the Documents.
    """
    return map(self.loadDoc, self.result[u"docs"])


class MapReduceQuery(object):
  """A wrapper around RiakMapReduce to play nice with Document

  Attributes:
    cls: The class for this MapReduceQuery.
    mr_obj: The original RiakMapReduce object.
    riak_links: All the links returned from the run operation of RiakMapReduce.
  """
  def __init__(self, cls, mr_obj):
    self.cls = cls
    self.mr_obj = mr_obj
    self.riak_links = mr_obj.run()

  def run(self):
    """A generator that goes through riak_link"""
    for link in self.riak_links:
      yield self.cls.load(link.get())

  def length(self):
    """The number of objects in this query.

    Deprecated. Use len() instead.

    Return:
      an integer that is the length of riak_obj
    """
    return len(self.riak_links)

  __len__ = length

  def all(self):
    """Returns all the Documents in a single list.

    Returns:
      A list containing all the Documents
    """
    return map(lambda link: self.cls.load(link.get()), self.riak_links)


def search(cls, bucket, querytext, client=None):
  """Searches through the bucket with some query text.

  The bucket must have search installed via search-cmd install BUCKETNAME.

  Args:
    cls: The class used to construct the final object.
    bucket: The bucket to search at. Can be different from the cls bucket.
    querytext: The query text as outlined in the python-riak documentations.
    client: Custom client if we don't want to use cls's client.

  Returns:
    A MapReduceQuery object. Similar to the RiakMapReduce object."""
  client = client or cls.client
  query_obj = client.search(bucket, querytext)
  return MapReduceQuery(cls, query_obj)

def solrSearch(cls, bucket, querytext, client=None, **kwargs):
  """Searches through using the SOLR.

  Args:
    cls: The class used to construct the final object.
    bucket: The bucket to SOLR. Can be different from cls's bucket
    querytext: The query text
    kwargs: Any other keyword arguments for SOLR.
    client: Custom client if we don't want to use cls's client.

  Returns:
    A SolrQuery object. Similart to a MapReduceQuery"""
  client = client or cls.client
  return SolrQuery(cls, client.solr().search(bucket, querytext, **kwargs))

def indexLookup(cls, bucket, index, startkey, endkey=None, client=None):
  """Short hand for creating a new mapreduce index

  Args:
    cls: The class used to construct the final object.
    bucket: The bucket to index. Can be different from cls's bucket
    index: The index field
    startkey: The starting key
    endkey: The ending key. If not none, search a range. Default: None
    client: Custom client if we don't want to use cls's client.

  Returns:
    A MapReduceQuery object
  """
  client = client or cls.client
  return MapReduceQuery(cls, client.index(bucket, index, startkey, endkey))