#!/bin/env python2.6

from couchdb import client, schema
from datetime import datetime
import hashlib
import re

class TimestampDocument(schema.Document):
    ts = schema.DateTimeField(default=datetime.now)

class Agent(TimestampDocument):
    type = schema.TextField(default='agent')
    message_ids = schema.ListField(schema.TextField())
    #domain_ids = schema.ListField(schema.TextField())
    #refresh_times = schema.ListField(schema.TextField())

class User(Agent):
    type = schema.TextField(default='user')
    username = schema.TextField()
    hashpass = schema.TextField()
    upvote_ids = schema.ListField(schema.TextField())
    influence = schema.DictField(schema.Schema.build())
#    influence = schema.DictField(schema.Schema.build(), default=int()) #why fails?
#     upvotes = schema.ListField(schema.DictField(schema.Schema.build( #optomization ?
#                 msg_id = schema.TextField(),
#                 ts = schema.DateTimeField()
#                 )))
#     @schema.View.define('user')
#     def by_name(doc):
#         yield doc['username'], doc
    by_name = schema.View('user', '''\
             function(doc) {
                 emit(doc.username, doc);
             }''')

class Message(TimestampDocument):
    agent_id = schema.TextField()
    title = schema.TextField()
    body = schema.TextField()
    links = schema.ListField(schema.TextField())
    channels = schema.ListField(schema.TextField())
    addressee_ids = schema.ListField(schema.TextField())
    upvote_ids = schema.ListField(schema.TextField())
#     upvotes = schema.ListField(schema.DictField(schema.Schema.build( #optomization ?
#                 agent_id = schema.TextField(),
#                 ts = schema.DateTimeField()
#                 )))
    parent_id = schema.TextField() # None unless self() is response
    response_ids = schema.ListField(schema.TextField())

    def hours_old(self):
        (dt.now() - self.ts).seconds/60/60

class Vote(TimestampDocument):
    """ 
    Agree: domain, article, author, other recommender agents
    Recommend: influence "sinks" (followers)
    """
    agent_id = schema.TextField()
    msg_id = schema.TextField()

class UpVote(Vote):
    type = schema.TextField(default='upvote')

class DownVote(Vote):
    type = schema.TextField(default='downvote')

class Visit(Vote):
    type = schema.TextField(default='visit')

class AgentDomain(TimestampDocument):
    """ Owned by an agent for purposes of influence 
    Could be extended to include unique URLs for some blogs, etc."""
    type = schema.TextField(default='agent_domain')
    link = schema.TextField()
    agent_id = schema.TextField()

class Tag(TimestampDocument):
    type = schema.TextField(default='tag')
    message_ids = schema.ListField(schema.TextField())

class Channel(Tag):
    type = schema.TextField(default='channel')
    #name = schema.TextField()

class Link(Tag):
    type = schema.TextField(default='link')
    #url = schema.TextField()

LINK_RE = re.compile("http[s]?://[\S]+", re.IGNORECASE) # http://www.ietf.org/rfc/rfc3986.txt
CHANNEL_RE = re.compile("#(\w+)", re.IGNORECASE)
ADDRESSEE_RE = re.compile("@(\w+)", re.IGNORECASE)

def parse_links(text):
    return LINK_RE.findall(text)

def parse_channels(text):
    return CHANNEL_RE.findall(text)

def parse_addressess(text):
    return ADDRESSEE_RE.findall(text)

def extract_tags(text):
    return {"links": parse_links(text), "channels": parse_channels(text)}

def create_user(db, username, password):
    #todo handle username already exists case
    user = User(username=username, hashpass=hashlib.sha224(password).hexdigest())
    user.store(db)
    return user

def create_message(db, agent, text):
    raw_tags = extract_tags(text)
    msg = Message(agent_id=agent.id, body=text, 
                  channels=raw_tags['channels'], links=raw_tags['links'])
    msg.store(db)
    agent.message_ids.append(msg.id)
    agent.store(db)
    for link in [get_create_link(db, url) for url in msg.links]:
        link.message_ids.append(msg.id)
        link.store(db)
    for chan in [get_create_channel(db, name) for name in msg.channels]:
        chan.message_ids.append(msg.id)
        chan.store(db)
    return msg

def _create_message(db, agent, title, link, channels, body):
    channel = get_channel(db, link)
    if channel:
        # already exists, so agreement with owning agent
        pass
    else:
        # create now and assign ownership to agent
        pass
    msg = Message(agent_id=agent.id, title=title, channels=channels, body=body, link=link)
    msg.store(db)
    agent.message_ids.append(msg.id)
    agent.store(db)
    return msg

def get_create_tag(db, id, type='tag'):
    tag = Tag.load(db, id)
    if tag:
        return tag
    tag = Tag(id=id, type=type)
    tag.store(db)
    return tag

def get_create_channel(db, name):
    return get_create_tag(db, name, type='channel')

def get_create_link(db, url):
    return get_create_tag(db, url, type='link')

def upvote_message(db, agent, msg):
    """ Queued process? """
    vote = UpVote(agent_id=agent.id, msg_id=msg.id)
    vote.store(db)
    #agent.upvotes.append({'msg_id':msg.id, 'ts': rating.ts})
    agent.upvote_ids.append(vote.id)
    agent.store(db)
    msg.upvote_ids.append(vote.id)
    msg.store(db)
    calculate_influence(db, msg, vote)
    return vote

def _respond_message(db, msg, agent, title, link, channels, body):
    resp = create_message(db, agent, title, link, channels, body)
    resp.parent_id = msg.id
    resp.store(db)
    msg.response_ids.append(resp.id)
    msg.store(db)
    return resp

def respond_message(db, msg, agent, text):
    resp = create_message(db, agent, text)
    resp.parent_id = msg.id
    resp.store(db)
    msg.response_ids.append(resp.id)
    msg.store(db)
    return resp

def calculate_influence(db, msg, vote):
    """ Queued process """
    owner = User.load(db, msg.agent_id)
    try:
        owner.influence[vote.agent_id] += 1
    except KeyError:
        owner.influence[vote.agent_id] = 1
    owner.store(db)

def score(msg, user):
    # todo: account for influence
    # negative influece ignored to prevent gaming system
    return len(msg.upvote_ids) - msg.hours_old()

def rank(msg_scores):
    """
    >>> sorted(d.iteritems(), key=itemgetter(1), reverse=True)
        [('b', 23), ('d', 17), ('c', 5), ('a', 2), ('e', 1)]
    >>> nlargest(2, d.iteritems(), itemgetter(1))
        [('b', 23), ('d', 17)]
    """
    return sorted(msg_scores, key=msg_scores.__getitem__, reverse=True)

import unittest

DB_NAME = 'splice-tests'
# probably want separate dbs for agents, messages, votes, and tags
# but not sure yet how that will affect views, etc.

class ParseTest(unittest.TestCase):

    def test_parse_links_embedded(self):
        links = parse_links('blah http://foo blah blah hTTps://bar.com blah')
        self.assertTrue(2, len(links))

    def test_parse_links_corners(self):
        links = parse_links('http://foo blah blah TTps://bar.com')
        self.assertTrue(1, len(links))
        links = parse_links('http://foo blah blah HTTps://bar.com')
        self.assertTrue(2, len(links))

    def test_parse_channels_embedded(self):
        chans = parse_channels('blah #food blah blah #clothes blah')
        self.assertTrue(2, len(chans))
        self.assertEqual("food", chans[0])

    def test_parse_channels_corners(self):
        chans = parse_links('#food blah blah buildings')
        self.assertTrue(1, len(chans))
        links = parse_links('#finecuisine blah blah #catering')
        self.assertTrue(2, len(chans))

    def test_extract_tags(self):
        text = "http://couchdb.org/id/1234 This article is great! #erlang #couchdb"
        tags = extract_tags(text)
        self.assertEqual(2, len(tags['channels']))
        self.assertEqual('http://couchdb.org/id/1234', tags['links'][0])



class DBTest(unittest.TestCase):

    def setUp(self):
        uri = os.environ.get('COUCHDB_URI', 'http://localhost:5984/')
        self.server = client.Server(uri)
        if DB_NAME in self.server:
            del self.server[DB_NAME]
        self.db = self.server.create(DB_NAME)

#     def tearDown(self):
#         """ Comment out for debugging """
#         if DB_NAME in self.server:
#             del self.server[DB_NAME]

    def test_get_create_tag(self):
        tag = get_create_channel(self.db, 'cats')
        self.assertEqual('channel', tag.type)
        self.assertEqual('cats', tag.id)

    def test_get_create_link(self):
        tag = get_create_link(self.db, 'http://example.com')
        self.assertEqual('link', tag.type)
        self.assertEqual('http://example.com', tag.id)

    def test_agent_creation(self):
        agent = Agent()
        assert agent.ts is not None
        assert agent.id is None
        agent.store(self.db)
        assert agent.id is not None

    def test_create_user(self):
        user = create_user(self.db, 'name', 'password')
        assert user.id is not None
        self.assertEqual('name', user.username)

    def test_user_by_name(self):
        user = create_user(self.db, 'myname', 'password')
        res = User.by_name(self.db, include_docs=True)
        print len(res.rows)

    def test_create_simple_message(self):
        user = create_user(self.db, 'name', 'password')
        assert user.id is not None
        msg = create_message(self.db, user, "Official python site http://python.org #python")
        assert msg.id is not None
        self.assertEqual(msg.id, user.message_ids[0])
        assert 'python' in msg.channels
        assert 'http://python.org' in msg.links

    def test_create_multichannel_message(self):
        user = create_user(self.db, 'name', 'password')
        assert user.id is not None
        msg = create_message(self.db, user, "#erlang Interesting concept in data storage http://couchdb.org #json")
        assert msg.id is not None
        self.assertEqual(msg.id, user.message_ids[0])
        assert 'erlang' in msg.channels
        assert 'json' in msg.channels
        assert 'http://couchdb.org' in msg.links

    def test_create_multilink_message(self):
        user = create_user(self.db, 'name', 'password')
        assert user.id is not None
        msg = create_message(self.db, user, "http://blog.poundbang.in/post/132952897/couchdb-naked http://damienkatz.net/2008/02/incremental_map.html are two interesting links on #couchdb, #erlang and #twitter ")
        assert msg.id is not None
        self.assertEqual(msg.id, user.message_ids[0])
        assert 'erlang' in msg.channels
        assert 'couchdb' in msg.channels
        assert 'twitter' in msg.channels
        self.assertEqual(2, len(msg.links))

    def test_upvote_message(self):
        user = create_user(self.db, 'user', 'password')
        user2 = create_user(self.db, 'user2', 'password')
        msg = create_message(self.db, user, "Official python site http://python.org #python")
        vote = upvote_message(self.db, user2, msg)
        assert vote.id is not None
        user = User.load(self.db, user.id) # ensure latest version
        self.assertEqual(1, user.influence[user2.id])

    def test_respond_message(self):
        user = create_user(self.db, 'user', 'password')
        user2 = create_user(self.db, 'user2', 'password')
        msg = create_message(self.db, user, "Official python site http://python.org #python")
        resp = respond_message(self.db, msg, user2, "First Post!")
        self.assertEqual(msg.id, resp.parent_id)

class RankTest(unittest.TestCase):
    
    def test_rank(self):
        self.assertEqual(['456', '123', '789'], rank({'123':1, '456':2, '789':0}))


def testmain():
    """ Useful for running from emacs. """
    try: unittest.main() 
    except SystemExit: pass

def main():
    pass

if __name__ == '__main__':
    testmain()
    #main()
