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

Licensed under LGPL

Installation
============

Requires the **LATEST** version of python-riak from
https://github.com/basho/riak-python-client .. It seems that Basho is not very
quick on releasing the newest and greatest feature in their tags...

Also, you need to change the setting of search to enable in your app.config

    {riak_search, [
      {enabled, false}
    ]}

This is if you want to use search.

Then, proceed to do `pip install riakkit` or `easy_install riakkit`.

This will not ensure that python-riak is installed. Please make sure you
install using the latest version from the repository (not a tag, latest).
The python-riak client is otherwise out of date.

"Fast Track"
============

Using riakkit should be simple. Here's how to get started.

    >>> from riakkit import Document, types
    >>> import riak
    >>> some_client = riak.RiakClient()
    >>> class BlogPost(Document):
    ...     # bucket name is required for each subclass of Document, unless you
    ...     # are extending Document.
    ...     # Each class gets their unique bucket_name.
    ...     bucket_name = "test_blog"
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
    ...     bucket_name = "test_users"
    ...     client = some_client
    ...
    ...     SEARCHABLE = True  # Marking this to be searchable.
    ...
    ...     name = types.StringProperty(required=True)
    ...     posts = types.LinkedDocuments()
    >>> user = User(name="mrrow")
    >>> some_post = BlogPost(title="Hello", content="World")
    >>> user.posts = [] # Initially this is set to nothing. So it's a good idea to initialize it.
    >>> user.posts.append(some_post)
    >>> some_post.save() # We must save this first, because we can't link to an object that doesn't exist yet.
    >>> user.save()
    >>> print user.posts[0].title
    Hello
    >>> same_user = User.get_with_key(user.key)
    >>> print same_user.posts[0].title
    Hello

LinkedDocuments is always a list.

You can also "back reference" these linked documents. The API is similar to
Google App Engine's `ReferenceProperty`, except everything is a list.

    >>> class Comment(Document):
    ...     bucket_name = "test_comments"
    ...     client = some_client
    ...
    ...     SEARCHABLE = True
    ...
    ...     title = types.StringProperty()
    ...     owner = types.LinkedDocuments(reference_class=User,
    ...                                   collection_name="comments")

Note how we specified the `reference_class`. This will activate additional
validation. Also, `collection_name` knows where to go.

    >>> a_comment = Comment(title="Riakkit ftw!", owner=[])
    >>> a_comment.owner.append(user) # Since there's only 1 owner, we will just leave 1 element.
    >>> a_comment.save()

This should save both the `a_comment`, and the `user` object. So no need to
`user.reload()`. However, you could reload `same_user`.

    >>> print user.comments[0].title
    Riakkit ftw!
    >>> same_user.reload()
    >>> print same_user.comments[0].title
    Riakkit ftw!


Let's add another comment.

    >>> another_comment = Comment(title="Moo", owner=[])

This time let's try having 2 owners. Even though this doesn't make any sense,
let's try it anyway.

    >>> another_owner = User(name="anotheruser")
    >>> another_owner.save()
    >>> another_comment.owner.append(same_user)
    >>> another_comment.owner.append(another_owner)
    >>> another_comment.save()
    >>> print same_user.comments[1].title
    Moo
    >>> print User.get_with_key(another_owner.key).comments[0].title
    Moo

Now, as of the moment, we lose order (potentially) when we store LinkedDocuments.
This means there's no guarentee that the comments will be at index 0 or 1.

    >>> user.reload()
    >>> titles = sorted([comment.title for comment in user.comments])
    >>> print titles
    [u'Moo', u'Riakkit ftw!']

Saving the comment multiple times shouldn't create multiple links in the user:

    >>> another_comment.title = "Abc"
    >>> another_comment.save()
    >>> user.reload()
    >>> titles = sorted([comment.title for comment in user.comments])
    >>> print len(titles)
    2
    >>> print titles
    [u'Abc', u'Riakkit ftw!']

`LinkedDocuments` store their properties in the object's meta information. This
may not be the best way to store things. Sometimes you want to use
`ReferenceProperty` and `MultiReferenceProperty`. These properties stores the
key of the object as a field in the JSON document. They have the same
capabilities as `LinkedDocuments` (`MultiReferenceProperty` is actually
identical to LinkedDocuments instead of the technical differences). They have
the same limitations as well.

Note that `ReferenceProperty` is NOT a list of documents, rather, it is only 1
document.

Note that `ReferenceProperty` and `MultiReferenceProperty` requires a
`reference_class`.

Let's take a look some examples:


    >>> class Person(Document):
    ...     bucket_name = "test_person"
    ...     client = some_client
    ...
    ...     name = types.StringProperty()
    >>> class Cake(Document):
    ...     bucket_name = "test_cake"
    ...     client = some_client
    ...
    ...     type = types.EnumProperty(["chocolate", "icecream"])
    ...     owner = types.ReferenceProperty(reference_class=Person, collection_name="cakes")
    >>> person = Person(name="John")
    >>> cake = Cake(type="chocolate")
    >>> cake.owner = person
    >>> cake.save()
    >>> print cake.owner.name
    John
    >>> print person.cakes[0].type
    chocolate

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

### Solr Search ###

Solr search allows you to do limit and sorting.

    >>> query = Comment.solrSearch("title:[A TO z]", sort="title")
    >>> print query.length()
    2
    >>> print sorted([comment.title for comment in query.all()])
    [u'Abc', u'Riakkit ftw!']

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

Other data types and validation
-------------------------------

Some different data types can also be used:

    >>> from datetime import datetime
    >>> class Demo(Document):
    ...     bucket_name = "demos"
    ...     client = some_client
    ...
    ...     # Let's throw in a validator. It makes sure all elements in the list
    ...     # is an integer.
    ...     test_list = types.ListProperty(validators=lambda x: len(x) == len([i for i in x if isinstance(i, int)]))
    ...     test_dict = types.DictProperty()
    ...     some_date = types.DateTimeProperty()
    ...     levels = types.EnumProperty(possible_values=["user", "admin"])
    >>>
    >>> demo_obj = Demo(test_list=[1, 2, "this causes failure"]) #doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
      ...
    ValueError: Validation did not pass for ...

Let's do it right this time.

    >>> demo_obj = Demo()

Here is a list and a dictionary.

    >>> demo_obj.test_list = [0, 1, 2]
    >>> demo_obj.test_dict = {"hello" : "world", 42 : 3.14}

Note that list and dictionaries are potentially dangerous as there's no type
checking in them. Refer to python's `json` to see objects are mapped.

`DictProperty` actually converts your dictionary to the class
`DictProperty.DotDict`. This class is a child class of python's `dict` and all
it does extra is allow you to access the attributes of the dictionary via the
dot notation. For example the "world" can be accessed via both
`demo_obj.test_dict["hello"]` and `demo_obj.test_dict.hello`. However,
non-string property may pose an issue.

    >>> print demo_obj.test_dict["hello"]
    world
    >>> print demo_obj.test_dict.hello
    world

Here's the `DateTimeProperty`

    >>> demo_obj.some_date = datetime(2011, 12, 16) # Just use a date time object

You can use the datetime object or an unix timestamp. Note that all datetime
handling is in utc. So riakkit you entered the utc time.

The `EnumProperty` basically is a list of possible values. If you feed it a
not allowed value, it will fail validation. The implementation of the
EnumProperty stores an integer corresponding to the location on the list you
specified. In this example, internally, "user" will have a value of 0 and
"admin" will have a value of 1. Note that these 2 strings will _not_ be
converted to unicode. EnumProperty accepts anything, but if it is passed
something like an object, you'll get the identical object back.

    >>> demo_obj.levels = "notpossible" #doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
      ...
    ValueError: Validation did not pass for ...
    >>> demo_obj.levels = "user"

Now let's save the object.

    >>> demo_obj.save()

We can now retrieve it again and see if this worked.

    >>> same_demo = Demo.get_with_key(demo_obj.key)
    >>> print same_demo.test_list
    [0, 1, 2]
    >>> print same_demo.test_dict.hello
    world
    >>> print sorted(same_demo.test_dict.items()) # this is done so that the doctest won't hate me.
    [(u'42', 3.14), (u'hello', u'world')]
    >>> print same_demo.some_date.year, same_demo.some_date.month, same_demo.some_date.day
    2011 12 16
    >>> print same_demo.levels
    user


Notice how the key of 42 (integer) got converted to u'42' (unicode). This is due
to JSON only allowing strings as keys.

For all the types, see the (API docs)[http://ultimatebuster.github.com/riakkit].

Uniqueness
----------

Uniqueness in Riakkit is enforced by creating an object in another bucket. The
bucket's name is generated as _<class bucket name>_ul_<property name>.

Let's construct a class:

    >>> class CoolUser(Document):
    ...     bucket_name = "coolusers"
    ...     client = some_client
    ...
    ...     username = types.StringProperty(unique=True)

This unique will create another bucket named "_coolusers_ul_username". Inside
this bucket, each object's key will be the values of the username. The value for
the object is {key : <the key of the document>}. Let's see how that works.

    >>> cooluser = CoolUser(username="cool")
    >>> cooluser.save()  # This is done successfully
    >>> notsocooluser = CoolUser(username="cool")
    >>> notsocooluser.save()
    Traceback (most recent call last):
      ...
    ValueError: 'cool' already exists for 'username'!
    >>> notsocooluser.saved()
    False
    >>> anothercooluser = CoolUser(username="anotheruser")
    >>> anothercooluser.save()  # This is done successfully


Advanced stuff
--------------

### Extending Document ###

If you got tired of writing `client = <yourclient>` everywhere. You can extend
the Document class. In order to do so, omit the `bucket_name` property.
You can also add other methods and variables, like any type of subclassing.

So:

    >>> class CustomDocument(Document):
    ...     client = some_client
    ...     another_property = True
    >>>
    >>> class SomeOtherDocument(CustomDocument):
    ...     bucket_name = "some_bucket"

    >>> print SomeOtherDocument.client == some_client
    True
    >>> print SomeOtherDocument.another_property
    True

### Validators and processors ###

You can use certain built-in validators, such as the email validators (or we
could write our own). This validates the data.

We could also use processors, this transforms the data. There are 2 types of
processors. The `forwardprocessors`, which are processors that transforms the
data before saving into the database, and before the `convert()` call. The
`backwardprocessors`, which are processors that transforms the data directly
fed from the database.

There are a certain number of built-in processors, such as automatically hashing
passwords. However, this still require you to write an inline function as
processor functions only take in 1 argument (a value) and returns the
transformed value. The hashing password function takes in a salt along with
a password, so a lambda function would help you. For more info, go see the
documentations.

In the mean while, demo time:

    >>> from riakkit.validators import emailValidator
    >>> class TestDocument(Document):
    ...     client = some_client
    ...     bucket_name = "testdoc"
    ...
    ...     email = types.StringProperty(validators=emailValidator)
    ...     some_property = types.IntegerProperty(standardprocessors=lambda x: x + 1)
    >>> test = TestDocument()
    >>> test.email = "notvalid" #doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
      ...
    ValueError: Validation did not pass for ...
    >>> test.email = "hello@world.com" # This works
    >>> test.some_property = 1
    >>> test.save()
    >>> print test.email
    hello@world.com
    >>> print test.some_property
    2

Use this feature responsibly.

Here's the work flow:

  1. `standardprocessors` are processors that takes values that's set by the
     users (They are fired by the `__setattr__`). These should not be fired when
     the objects are loaded/reloaded from the database.
  2. `forwardprocessors` are processors that takes the value that's already
     "standardized" and converts it into database friendly format. (or friendly
     to `backwardprocessors`)
  3. `backwardprocessors` are processors that takes the value that's obtained
     from the database and converts it back to a format from the database back
     to an usuable format. The value it returns should be friendly to
     `standardprocessors`.

So given this our example would be bad practise. We should also implement a
`backwardprocessors` of `lambda x: x - 1` so that the x value don't keep
incrementing. Unless that's what you want to do. Make sure you're responsible
when doing this as it could cause some weird bugs like the following:

    >>> test.reload()
    >>> print test.some_property
    2

**Warning: This is an experimental feature. This may change in the future as
there are concerns that this is too complicated.**


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
