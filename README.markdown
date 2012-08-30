Riakkit
=======

Google Groups/Mailing list: https://groups.google.com/forum/#!forum/riakkit or riakkit@googlegroups.com

What is this?..
===============

Riakkit is an object mapper and a RAD tool for Riak.

Riakkit underwent a quite significant change from 0.5 to 0.6 (notice we went
back to Alpha). There's a lot of backend changes, although *most* front end
methods are unchanged. Additional API is also introduced.

Just FYI: The project tries to follow the [Google Python style guide](http://google-styleguide.googlecode.com/svn/trunk/pyguide.html).

Licensed under LGPLv3

[![Donate to me to keep this going!](https://www.paypalobjects.com/en_US/i/btn/btn_donate_SM.gif)](https://www.paypal.com/cgi-bin/webscr?cmd=_donations&business=FGWYWWS4CJJFW&lc=CA&item_name=Riakkit&item_number=riakkit&currency_code=CAD&bn=PP%2dDonationsBF%3abtn_donate_SM%2egif%3aNonHosted)

Installation
============

Requires version 1.4.0 or above of [riak-python-client](https://github.com/basho/riak-python-client).
Probably best if you just grab the repository version.

Also, you need to change the setting of search to enable in your app.config

    {riak_search, [
      {enabled, false}
    ]}

This is if you want to use search.

Then, proceed to do `pip install riakkit` or `easy_install riakkit`.

Concept
=======

There's 2 parts to Riakkit. `riakkit.SimpleDocument` and `riakkit.Document`.
`Document` is a subclass of `SimpleDocument` and it provides the RAD capabilities.
This means `Document` helps you track dependencies, make sure everything is
and allow you to write a prototype in days without much effort, dealing with
all the issues with tracking different models and how they relate to each other.

This, however, comes with a cost. `Document` has a lot of overhead. There's a
lot of code present that slows the program down (I'm working on improving it,
but it's not the topmost priority right now). This is why when you need to start
scaling, it's recommended that you stop using `Document` for your models.

This is where `SimpleDocument` comes into play. `SimpleDocument` does not
communicate with Riak. That means all the convinience methods such as `save` and
`delete` is not available any more. However, many methods are still available,
like `addLinks`, `indexes` and all that. Along with many methods you may not
even know exists, such as `toRiakObject`. If you use this, it's your
responsibility to track relationships (this may change, depending on if
an efficient way to track relationships comes along or not) and save all the
objects to database.

Since all it really does is validate and convert values if necessary,
`SimpleDocument` is very very fast. If you ever run the unittest yourself, you
will actually see a difference. This also comes at a cost. You will have to
track the relationships.

You could also subclass SimpleDocument and make it like `Document`, but with
less overhead. If you come up with something that's almost exactly like
`Document` but faster, please make a pull request! (LGPL don't require you to
do so, but hey, it's nice to do it)

"Fast Track"
============

Using riakkit with the higher level API should be simple.
Here's how to get started:

    >>> from riakkit import *
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
    ...     title = StringProperty(required=True) # StringProperty auto converts all strings to unicode
    ...     content = StringProperty() # let's say content is not required.
    ...     some_cool_attribute = FloatProperty() # Just a random attribute for demo purpose
    ...     def __str__(self): # Totally optional..
    ...         return "%s:%s" % (self.title, self.content)

Make sense, right? We imported riakkit and riak, created a connection, and a Document subclass.

    >>> post = BlogPost(title="hi")
    >>> print post
    hi:None
    >>> post.save() #doctest: +ELLIPSIS
    <__main__.BlogPost object at ...>

Saving is easy, but how do we modify?

    >>> post.title = "Hello"
    >>> post.content = "mrrow"
    >>> post.save() #doctest: +ELLIPSIS
    <__main__.BlogPost object at ...>
    >>> print post
    Hello:mrrow
    >>> key = post.key # Stores a key...

Since the title is required.. we cannot save if it's not filled out.

    >>> another_post = BlogPost(content="lolol")
    >>> another_post.save()
    Traceback (most recent call last):
        ...
    ValidationError: None doesn't pass validation for property 'title'

What about getting it from the database?

    >>> same_post = BlogPost.get(key, False) # False means that we don't want the cached copy, but reload the object from the database even if it''s available in cache
    >>> print same_post
    Hello:mrrow

All object that's constructed using `Document` that's been `get` are the
**same** instance. There's one object per key. Any changes to
the object will be reflected in all the references to it. A
`WeakValueDictionary` is used to cache all the objects.

    >>> same_post is post
    True

However, if your data got modified outside of riakkit, you could use the
`.reload()` function for document objects.

    >>> same_post.reload() # Obviously we haven't changed anything, but if we did, this would get those changes
    >>> print same_post.title
    Hello

You can also use dictionary notation. However, there's Document is not a
superclass of dict!

    >>> print same_post.title
    Hello
    >>> print same_post["title"]
    Hello

Need another attribute not in your schema? No problem.

    >>> same_post.random_attr = 42
    >>> same_post.save() # doctest: +ELLIPSIS
    <__main__.BlogPost object at ...>
    >>> print same_post.random_attr
    42

Again, you can see the changes are instantly reflected on the other reference
to it:

    >>> print post.random_attr
    42

While setting an attribute in your schema is allowed, getting one while it's not
in the scheme **AND** not already set will raise an AttributeError.

    >>> same_post.none_existent
    Traceback (most recent call last):
      ...
    AttributeError: Attribute none_existent not found with BlogPost.

Accessing an attribute that's **IN** your schema but **NOT** set will return
`None`, or whatever default value you got. (Some properties already have a
default value. Example: if you don't set a `ListProperty`, it will return [] if
you get it)

    >> print same_post.some_cool_attribute  # Remember? We never set this
    None

Deleting objects is equally as easy.

    >>> same_post.delete()
    >>> BlogPost.get(key) #doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
        ...
    NotFoundError: Key '<yourkey>' not found!


Referencing Documents
---------------------

You can link to a "foreign" document very easily. Let me illustrate:

    >>> class User(Document):
    ...     bucket_name = "doctest_users"
    ...     client = some_client
    ...
    ...     name = StringProperty(required=True)
    ...     post = ReferenceProperty(reference_class=BlogPost)
    >>> user = User(name="mrrow")
    >>> some_post = BlogPost(title="Hello", content="World")
    >>> user.post = some_post
    >>> user.save() # doctest: +ELLIPSIS
    <__main__.User object at ...>
    >>> print user.post.title
    Hello
    >>> same_user = User.load(user.key)
    >>> print same_user.post.title
    Hello

You can also "back reference" these documents. The API is similar to
Google App Engine's `ReferenceProperty`.

    >>> class Comment(Document):
    ...     bucket_name = "doctest_comments"
    ...     client = some_client
    ...
    ...     title = StringProperty()
    ...     owner = ReferenceProperty(reference_class=User,
    ...                               collection_name="comments")

Note how we specified the `reference_class`. This will activate additional
validation. Also, `collection_name` knows where to go.

    >>> a_comment = Comment(title="Riakkit ftw!")
    >>> a_comment.owner = user
    >>> a_comment.save() # doctest: +ELLIPSIS
    <__main__.Comment object at ...>

This should save both the `a_comment`, and the `user` object. So no need to
`user.reload()`. Since the `same_user` variable is just a reference to `user`,
there is no need to reload that, either (Behaviour introduced after v0.3.2a).

    >>> print user.comments[0].title
    Riakkit ftw!
    >>> print same_user.comments[0].title
    Riakkit ftw!

Let's add another comment.

    >>> another_comment = Comment(title="Moo")

Note that `ReferenceProperty` and `MultiReferenceProperty` requires a
`reference_class`.

Let's look at `MultiReferenceProperty`, it's very simple as it's just a list of
`Documents`

    >>> class Cake(Document):
    ...     bucket_name = "test_cake"
    ...     client = some_client
    ...
    ...     type = EnumProperty(["chocolate", "icecream"])
    ...     owner = MultiReferenceProperty(reference_class=User, collection_name="cakes")
    >>> person = User(name="John")
    >>> cake = Cake(type="chocolate", owner=[])
    >>> cake.owner.append(person)
    >>> cake.save() #doctest: +ELLIPSIS
    <__main__.Cake object at ...>
    >>> print cake.owner[0].name
    John
    >>> print person.cakes[0].type
    chocolate
    >>> cake.owner = [user]
    >>> cake.save() #doctest: +ELLIPSIS
    <__main__.Cake object at ...>
    >>> print person.cakes
    []
    >>> print cake.owner[0].name
    mrrow

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

    search-cmd install doctest_comments
    search-cmd install doctest_users

See more at http://wiki.basho.com/Riak-Search.html and
http://basho.github.com/riak-python-client/tutorial.html#using-search (You
 don't need the bucket name because it is provided with each class)

    >>> user_query = User.search("name:'mrrow'") # Searches for the user we created.
    >>> print user_query.length()
    1
    >>> for user in user_query.run():
    ...     print user.name # user is am User object.
    mrrow


### Solr Search ###

Solr search allows you to do limit and sorting.

    >>> query = Comment.solrSearch("title:[A TO z]", sort="title")
    >>> print query.length()
    1
    >>> print sorted([comment.title for comment in query.all()])
    [u'Riakkit ftw!']


### Riak 2i ###

After v0.3.2a, Riakkit gained support for easier Riak 2i. The interface stayed
relatively the same, with the renaming of `add_index` to `addIndex` due to the
Google Python styling guide.

To add an index, simply do

    >>> cake.addIndex("field1_bin", "val1")#doctest: +ELLIPSIS
    <...>
    >>> cake.save() #doctest: +ELLIPSIS
    <...>

To get the indexes:

    >>> print cake.indexes("field1_bin")
    set(['val1'])
    >>> print cake.indexes()
    [('field1_bin', 'val1')]

To do an index based query:

    >>> query = Cake.indexLookup("field1_bin", "val1")
    >>> for cake in query.run():
    ...     print cake.type
    chocolate

For additional information, please checkout the API docs.

### Riak Links ###

To add a link:

    >>> user = User(name="John")
    >>> # cake is the previous chocolate cake we had.
    >>> user.addLink(cake)#doctest: +ELLIPSIS
    <...>
    >>> user.save()# doctest: +ELLIPSIS
    <...>

Fetching them is also as easy:

    >>> for cake, tag in user.links():
    ...     print cake.type
    chocolate

The API is identical to riak-python's API, only slightly altered to fit Google
Python Style Guide (`addLink` also takes a tag) and it takes/returns `Document`
instead of `RiakObject`. For additional information, please checkout the API
docs.

`removeLink` is used to remove links, and it takes either a `Document` instance
or a key string.

Like that, you could also add multiple links to an object with different
Documents. This is more flexible than `ReferenceProperty`

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
    ...     test_list = ListProperty(validators=lambda x: len(x) == len([i for i in x if isinstance(i, int)]))
    ...     test_dict = DictProperty()
    ...     some_date = DateTimeProperty()
    ...     levels = EnumProperty(possible_values=["user", "admin"])
    >>>
    >>> demo_obj = Demo(test_list=[1, 2, "this causes failure"]) #doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
      ...
    ValidationError: Validation did not pass for ...

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

You can use the datetime object or an unix timestamp.

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
    ValidationError: Validation did not pass for ...
    >>> demo_obj.levels = "user"

Now let's save the object.

    >>> demo_obj.save() #doctest: +ELLIPSIS
    <...>

We can now retrieve it again and see if this worked.

    >>> same_demo = Demo.get(demo_obj.key, False) # This False means that we want to reload the object even if it is cached
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

One more note on the `DateTimeProperty`: if you don't specify, it will use
`now` as the default value:

    >>> another_demo = Demo()
    >>> another_demo.save()#doctest: +ELLIPSIS
    <...>
    >>> print another_demo.some_date #doctest: +SKIP
    <This will print the date time this is ran>


Notice how the key of 42 (integer) got converted to u'42' (unicode). This is due
to JSON only allowing strings as keys.

For all the types, see the (API docs)[http://shuhaowu.github.com/riakkit].

Uniqueness
----------

Uniqueness in Riakkit is enforced by creating an object in another bucket. The
bucket's name is generated as _<class bucket name>_ul_<property name>.

Let's construct a class:

    >>> class CoolUser(Document):
    ...     bucket_name = "coolusers"
    ...     client = some_client
    ...
    ...     username = StringProperty(unique=True)

This unique will create another bucket named "_coolusers_ul_username". Inside
this bucket, each object's key will be the values of the username. The value for
the object is {key : <the key of the document>}. Let's see how that works.

    >>> cooluser = CoolUser(username="cool")
    >>> cooluser.save() #doctest: +ELLIPSIS
    <...>
    >>> notsocooluser = CoolUser(username="cool")
    >>> notsocooluser.save()
    Traceback (most recent call last):
      ...
    IntegrityError: 'cool' already exists for 'username'!
    >>> anothercooluser = CoolUser(username="anotheruser")
    >>> anothercooluser.save() #doctest: +ELLIPSIS
    <...>
    >>> anothercooluser.username = "cool"
    >>> anothercooluser.save()
    Traceback (most recent call last):
      ...
    IntegrityError: 'cool' already exists for 'username'!

Embedded Document
-----------------

Since we're working with NoSQL.. we can store complex data types, such as
a dictionary (or an array of dictionaries, to be implemented).

This is where `EmDocument` comes in handy. `EmDocument` acts like a `Document`,
but it is stored as just a plain old dictionary embedded inside a `Document`.

We fist need to extend `EmDocument` similar to extending `Document`, though we
don't need to specify `client` and `bucket_name` class variables.

    >>> from riakkit import EmDocument
    >>> class Admin(EmDocument):
    ...     email = StringProperty(required=True) # Required works, see demo later
    ...     level = EnumProperty(["regular", "super"], default="regular") # Default works as well.
    >>> class Page(EmDocument):
    ...     name = StringProperty()
    ...     content = StringProperty()

Then we need to use `EmDocumentProperty` and specify the `emdocument_class`.

    >>> class Website(Document):
    ...     client = some_client
    ...     bucket_name = "test_website"
    ...
    ...     name = StringProperty()
    ...     admin = EmDocumentProperty(emdocument_class=Admin)
    ...     pages = EmDocumentsListProperty(emdocument_class=Page) # demo'ed later

We can then use this, let's demo the `required` first:

    >>> the_admin = Admin()
    >>> the_website = Website(name="The Website", admin=the_admin)
    >>> the_website.save()
    Traceback (most recent call last):
        ...
    ValidationError: None doesn't pass validation for property 'email'

Alright, let's do this the right way this time:

    >>> the_admin.email = "admin@thekks.net"
    >>> print the_website.admin.email
    admin@thekks.net
    >>> # Save again as required. This save also kicks in the default values.
    >>> the_website.save() #doctest: +ELLIPSIS
    <...>
    >>> print the_website.admin.level
    regular
    >>> print the_admin.level
    regular
    >>> same_website = Website.get(the_website.key, False) # This False signifies that we don't want the cached copy and wants to reload it
    >>> print same_website.admin.email
    admin@thekks.net
    >>> print isinstance(same_website.admin, Admin)
    True
    >>> print same_website.admin.level
    regular

We could also just set a dictionary.

    >>> same_website.admin = {"email" : "email@example.com"}
    >>> print isinstance(same_website.admin, Admin)
    True
    >>> print same_website.admin.email
    email@example.com

Essentially everything works with `EmDocument` works for `Document`.
However, no methods (class or variable) is available other than setting
variables via attributes or keys.

Also, uniques are not allowed, reference properties cannot have collection_name
Required, however, works, as you have seen.

We could also use a list of EmDocuments as you probably have figured out by now:

    >>> same_website.pages = []
    >>> home = Page(name="Home", content="Hello World!")
    >>> same_website.pages.append(home)

You can also just append a dictionary. Via some magic it auto turns into a Page
instance.

    >>> same_website.pages.append({"name" : "About", "content" : "Riakkit!"})
    >>> same_website.save() #doctest: +ELLIPSIS
    <...>
    >>> print the_website.pages[0].name, the_website.pages[0].content
    Home Hello World!
    >>> print the_website.pages[1].name, the_website.pages[1].content
    About Riakkit!

You can add extra attributes to `EmDocument` just like you would with regular
`Document`.

    >>> same_website.pages.append({"name" : "Contact", "content" : "Contact us here!", "random_attr" : 1})
    >>> same_website.save() #doctest: +ELLIPSIS
    <...>
    >>> print the_website.pages[2].random_attr
    1

You can also use methods such as `extend`, `insert`, and just `[index]` like
`append`.

    >>> same_website.pages[2] = {"name" : "Products", "content" : "All our products."}
    >>> same_website.save() #doctest: +ELLIPSIS
    <...>
    >>> print isinstance(same_website.pages[2], Page)
    True
    >>> print the_website.pages[2].name
    Products

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
    ...     test_property = StringProperty()

    >>> print SomeOtherDocument.client == some_client
    True
    >>> print SomeOtherDocument.another_property
    True

You can also extend documents with bucket_name defined.

    >>> class ExtendedDocument(SomeOtherDocument):
    ...     bucket_name = "some_extended_bucket"
    ...
    ...     extended_property = IntegerProperty()
    >>> ed = ExtendedDocument()
    >>> print ed.test_property
    None
    >>> ed.test_property = "mrrow"
    >>> ed.save() #doctest: +ELLIPSIS
    <...>
    >>> ed1 = ExtendedDocument.load(ed.key) # load is the same as get(key, False)
    >>> print ed1.test_property
    mrrow

If we print `ed.test_property` and it's not registered and not in our data set,
it will raise an error instead of

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

    >>> from riakkit.helpers import emailValidator
    >>> class TestDocument(Document):
    ...     client = some_client
    ...     bucket_name = "testdoc"
    ...
    ...     email = StringProperty(validators=emailValidator)
    ...     some_property = IntegerProperty(standardprocessors=lambda x: x if x is None else x + 1)
    >>> test = TestDocument()
    >>> test.email = "notvalid" #doctest: +IGNORE_EXCEPTION_DETAIL
    Traceback (most recent call last):
      ...
    ValidationError: Validation did not pass for ...
    >>> test.email = "hello@world.com" # This works
    >>> test.some_property = 1
    >>> test.save() #doctest: +ELLIPSIS
    <...>
    >>> print test.email
    hello@world.com
    >>> print test.some_property
    2

Use this feature responsibly. For example, having multiple base classes could
pose a problem. The order of inheritance is via a BFS down the parents chain and
reversing it to update the properties.

Here's the work flow:

  1. `standardprocessors` are processors that takes values that's set by the
     users (They are fired by the `__setattr__`). These generally will
     not be fired when the objects are loaded/reloaded from the database.
  2. `forwardprocessors` are processors that takes the value that's already
     "standardized" and converts it into database friendly format. (or friendly
     to `backwardprocessors`)
  3. `backwardprocessors` are processors that takes the value that's obtained
     from the database and converts it back to a format from the database back
     to an usuable format. The value it returns should be friendly to
     `standardprocessors`.


It's very important that any processor and validators that you write can deal
with the None type and shouldn't process the None type, which is what we did.
The None type should also be able to pass any validation.

So given this our example would be bad practise. We should also implement a
`backwardprocessors` of `lambda x: x if x is None else x - 1` so that the x
value don't keep incrementing. Unless that's what you want to do. Make sure
you're responsible when doing this as it could cause some weird bugs like the
following:

    >>> test.reload()
    >>> print test.some_property
    2

**Warning: This is an experimental feature. This may change in the future as
there are concerns that this is too complicated.**

### Changing the schema ###

If you at one point saved a document, and then changed the schema in your code,
the next time the documents are retrieved from the database, riakkit will
automatically attach the default values of those types into the Document objects.
It won't automatically save the default values (which most are None, some are
[] and {}) into the database. That's up to you with a `.save()` call. However,
those properties won't always come up as None if you have default specified.

This is to make it easier for development.

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
http://shuhaowu.github.com/riakkit

That may be outta date though.. so I think you should build your own docs.

cd into this directory and do `./makedocs`

Until someone makes a prettier doc, that is (You should be glad I wrote docs,
'cause I usually don't do it).

Unittests
---------

Run `python test_all.py`.
