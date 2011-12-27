import flask
from settings import *
app = flask.Flask(__name__)
from models import *
from functools import wraps
import markdown
import re
from datetime import datetime
from riakkit.exceptions import NotFoundError

alphanumeric = re.compile(r"\w+")

app.secret_key = SECRET_KEY

@app.before_request
def before_request():
  app.jinja_env.globals["loggedin"] = "username" in flask.session

def loginRequired(f):
    @wraps(f)
    def df(*args, **kwargs):
        if not flask.session.get("username", False):
            return flask.redirect(flask.url_for("login"))
        return f(*args, **kwargs)
    return df


def constructDict(post_obj):
  return {
      "author" : post_obj.owner[0].dispname or post_obj.owner[0].username,
      "title" : post_obj.title,
      "content" : post_obj.content,
      "date" : post_obj.date.strftime("%b %d %Y %H:%M:%S"),
      "comment_count" : len(post_obj.comments),
      "key" : post_obj.key
  }

@app.route("/")
@app.route("/offset/<amt>")
def index(amt=0):
  amt = int(amt)
  post_objs = Post.solrSearch("title:[A TO z]", sort="date", start=amt)
  posts = []
  for post_obj in post_objs.run():
    posts.append(constructDict(post_obj))
    print post_obj.content
  posts.reverse()
  return flask.render_template("index.html", posts=posts,
      prev=amt-10 if amt-10 >= 0 else 0,
      next=amt+10 if amt <= post_objs.result[u"response"][u"numFound"] else post_objs.result[u"response"][u"numFound"]) # someone fix this o.o

@app.route("/view/<key>")
def viewpost(key):
  try:
    post = Post.getWithKey(key)
  except NotFoundError:
    return flask.abort(404)

  return flask.render_template("post.html", posts=[constructDict(post)], post=post, title=post.title)


@app.route("/login", methods=["GET", "POST"])
def login():
  error = None
  if "username" in flask.session:
    return flask.redirect(flask.url_for("admin"))

  if flask.request.method == "POST":
    username = flask.request.form["username"].strip()
    if not alphanumeric.match(username):
      error = "Invalid username."
    else:
      password = hashPassword(flask.request.form["password"])
      query = User.solrSearch("username:'%s' AND password:'%s'" % (username, password))
      if query.length() == 1:
        flask.session["username"] = username
        flask.session["userkey"] = query.all()[0].key
        return flask.redirect(flask.url_for("index"))
      else:
        error = "Incorrect username and password!"

  return flask.render_template("login.html", error=error, title="Login")

@app.route("/logout")
@loginRequired
def logout():
  del flask.session["username"]
  return flask.redirect(flask.url_for("index"))

@app.route("/admin")
@loginRequired
def admin():
  return flask.render_template("adminui.html", title="Admin UI")

@app.route("/admin/post", methods=["POST"])
@loginRequired
def postpost():
  title = flask.request.form["title"].strip()
  content = flask.request.form["content"]
  if title and content:
    htmlcontent = markdown.markdown(content)
    post = Post(title=title, content=htmlcontent, mdsrc=content, date=datetime.utcnow(), owner=[])
    userkey = flask.session["userkey"]
    post.owner.append(User.load(userkey))
    post.save()
    flask.flash("Post successfully updated!", "success")
    return flask.redirect(flask.url_for("admin"))
  else:
    flask.flash("Invalid title or content!", "error")
    return flask.redirect(flask.url_for("admin"))

@app.route("/admin/edit/<key>", methods=["GET", "POST"])
@loginRequired
def editpost(key):
  if flask.request.method == "GET":
    post = Post.getWithKey(key)
    return flask.render_template("updatepost.html", post=post)
  else:
    title = flask.request.form["title"].strip()
    content = flask.request.form["content"]
    post = Post.getWithKey(key)
    if title and content:
      htmlcontent = markdown.markdown(content)
      post.title = title
      post.content = htmlcontent
      post.mdsrc = content
      post.save()
      flask.flash("Post updated sucessfully!", "success")
      return flask.render_template("updatepost.html", post=post)
    else:
      flask.flash("Invalid post title or content!", "error")
      return flask.render_template("updatepost.html", post=post, title="Edit")


@app.route("/admin/delete/<key>")
@loginRequired
def deletepost(key):
  Post.getWithKey(key).delete()
  flask.flash("Post successfully deleted.", "success")
  return flask.redirect(flask.url_for("admin"))

if __name__ == "__main__":
  if DEBUG == True:
    app.run(debug=True, host="")
  else:
    from gevent.wsgi import WSGIServer
    http_server = WSGIServer(("127.0.0.1", DEPLOY_PORT), app)
    http_server.serve_forever()
