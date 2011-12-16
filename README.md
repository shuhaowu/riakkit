Riakkit
=======

What is this?..
===============

Riakkit is essentially an(other) object mapper for Riak. Meaning it's kind of
like mongokit or couchdbkit, where it makes it easier to map an object to Riak.
Initially a project designed to immitate mongokit, now it got inspired a lot by
[riakalchemy](https://github.com/Linux2Go/riakalchemy). This is designed to fit
a project of mine and my style of coding and use cases.

Just FYI: The project tries to follow the [Google Python style guide](http://google-styleguide.googlecode.com/svn/trunk/pyguide.html).

Licensed under GPLv3, it is much more restrictive than riakalchemy.. :D So
please share your code back to me.

Installation
============

setup.py is not yet available... Maybe down the road.. but just drop riakkit/
into your project and run with it..

Requires the **LATEST** version of python-riak from
https://github.com/basho/riak-python-client .. It seems that Basho is not very
quick on releasing the newest and greatest feature in their tags...

Also, you need to change the setting of search to enable in your app.config

    {riak_search, [
      {enabled, false}
    ]}

This is if you want to use search.


"Fast Track"
============

Using riakkit should be simple. Here's how to get started.

    >>> from riakkit import Document, types
    >>> import riak
    >>> some_client = riak.RiakClient()
    >>> class BlogPost(Document):
    ...     # bucket name is required for each subclass of Document.
    ...     # Each class gets their unique bucket_name.
    ...     bucket_name = "blog"
    ...
    ...     # Client is required for each subclass of Document
    ...     client = some_client
    ...
    ...     title = types.StringProperty(required=True) # StringProperty auto converts all strings to unicode
    ...     content = types.StringProperty() # let's say content is not required.
    ...     def __str__(self): # Totally optional..
    ...         return "%s:%s" % (self.title, self.content)

Make sense, right? We imported riakkit and riak, created a connection, and a Document subclass.

    >>> post = BlogPost(title="hi")
    >>> print post
    hi:None
    >>> post.saved() # see if the post is saved or not.
    False
    >>> post.save() # saves the post into the database
    >>> post.saved()
    True

Saving is easy, but how do we modify?

    >>> post.title = "Hello"
    >>> post.content = "mrrow"
    >>> post.saved()
    False
    >>> post.save()
    >>> print post
    Hello:mrrow
    >>> key = post.key # Stores a key...

Since the title is required.. we cannot save if it's not filled out.

    >>> another_post = BlogPost()
    >>> another_post.save()
    Traceback (most recent call last):
        ...
    AttributeError: 'title' is required for 'BlogPost'.

What about getting it from the database?

    >>> same_post = BlogPost.get_with_key(key)
    >>> print same_post
    Hello:mrrow

You can also use dictionary notation. However, there's Document is not a
superclass of dict!

    >>> print same_post.title
    Hello
    >>> print same_post["title"]
    Hello

Need another attribute not in your schema? No problem.

    >>> same_post.random_attr = 42
    >>> same_post.save()
    >>> print same_post.random_attr
    42
    >>> post_again = BlogPost.get_with_key(key)
    >>> print post_again.random_attr
    42

Deleting objects is equally as easy.

    >>> same_post.delete()
    >>> BlogPost.get_with_key(key) #doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    NotFoundError: Key '<yourkey>' not found!

Linked Documents
----------------

You can link to a foreign document very easily. Let me illustrate:

    >>> class User(Document):
    ...     bucket_name = "users"
    ...     client = some_client
    ...
    ...     SEARCHABLE = True  # Marking this to be searchable.
    ...
    ...     name = types.StringProperty(required=True)
    ...     posts = types.LinkedDocuments()
    >>> user = User(name="mrrow")
    >>> some_post = BlogPost(title="Hello", content="World")
    >>> user.posts = []
    >>> user.posts.append(some_post)
    >>> user.save()
    >>> print user.posts[0].title
    Hello
    >>> same_user = User.get_with_key(user.key)
    >>> print same_user.posts[0].title
    Hello

LinkedDocuments is always a list.

Advanced Query
--------------

### Searching ###

You've see getting with get_with_key, what about searching and map reduce?

Searching is done through Document.search(querytext). This required
enable_search to be on. Otherwise you're limited to map reduce.

Also, you're required to install the search onto buckets that you will use for
searching. The following command will do:

    search-cmd install BUCKETNAME

For this tutorial we will do

    search-cmd install users

See more at http://wiki.basho.com/Riak-Search.html and
http://basho.github.com/riak-python-client/tutorial.html#using-search (You
 don't need the bucket name because it is provided with each class)

    >>> user_query = User.search("name:'mrrow'") # Searches for the user we created.
    >>> print user_query.length()
    1
    >>> for user in user_query.run():
    ...     print user.name # user is am User object.
    mrrow

If you didn't mark the class as `SEARCHABLE`, you'll get a NotImplementedError.

    >>> BlogPost.search("title:'Hello'")
    Traceback (most recent call last):
      ...
    NotImplementedError: Searchable is disabled, this is therefore not implemented.

### Map Reduce ###

Map Reduce with Riakkit is the same to the python-riak's map reduce. In fact,
Riakkit only provides a short hand using `Document.mapreduce()`, which is
implemented as `return cls.client.add(cls.bucket_name)`. This saves you the
work of entering the bucket name, as you only need to call it with your class.

Please see their documentations for how to use it.

An alternate way should be done in the future to automatically create Document
objects from a special map reduce. However, since map reduce could return all
sorts of data, not just Documents.

If you need to construct a document from the map reduced data to take advantage
of the ease of handling with riakkit, you can construct the object like this
(replace Document with your class name, of course):

    Document(your_key, True, **json_data)

 * `your_key` is the key of the data.
 * True signifies that this data has been saved.
 * json_data is the field values for this object.

Note: If json_data is actually different from the data in the database, you
might want to call `.save()` again. This type of import assumes the data is
saved, so a `.saved()` call will return True if you didn't modify anything, and
a `.reload()` call will overwrite your data with the values in the db.


Other thingies
==============

Some different data types can also be used:

    >>> class Demo(Document):
    ...     bucket_name = "demos"
    ...     client = some_client
    ...
    ...     # Let's throw in a validator. It makes sure all elements in the list
    ...     # is an integer.
    ...     test_list = types.ListProperty(validators=lambda x: len(x) == len([i for i in x if isinstance(i, int)]))
    ...     test_dict = types.DictProperty()
    >>>
    >>> demo_obj = Demo(test_list=[1, 2, "this causes failure"]) #doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
      ...
    ValueError: Validation did not pass for ...

Let's do it right this time.

    >>> demo_obj = Demo()
    >>> demo_obj.test_list = [0, 1, 2]
    >>> demo_obj.test_dict = {"hello" : "world", 42 : 3.14}
    >>> demo_obj.save()
    >>> same_demo = Demo.get_with_key(demo_obj.key)
    >>> print same_demo.test_list
    [0, 1, 2]
    >>> print sorted(same_demo.test_dict.items()) # this is done so that the doctest won't hate me.
    [(u'42', 3.14), (u'hello', u'world')]

Notice how the key of 42 (integer) got converted to u'42' (unicode). This is due
to JSON only allowing strings as keys.

Accessing Underlying Riak API
=============================

The relationship between Riakkit and Python-riak is an interesting one. On the
one hand, it's recommended that you do not create objects manually using riak
if you want to use riakkit. That might cause some strangeness. However, getting
data is easily doable. Here are some entry ways (again, it's your class name):

 * `Document.client` is the client. You specified this so you should know.
 * `Document.bucket` is the RiakBucket for this client with the bucket name of
   `Document.bucket_name`.
 * `Document.mapreduce()` is simply `Document.client.add(Document.bucket_name)`

You can find more information on [python-riak's](https://github.com/basho/riak-python-client)
page.


More Info on Riakkit
====================

API Docs
--------

cd into this directory and cd into the docs directory should do! Or visit 
http://ultimatebuster.github.com/riakkit 

That may be outta date though.. so I think you should build your own docs.

cd into this directory and do `./makedocs`

Until someone makes a prettier doc, that is (You should be glad I wrote docs,
'cause I usually don't do it).

Doctests
--------

Run `python runtests.py`.
