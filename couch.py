#!/bin/env python2.6

from couchdb import client, schema
from datetime import datetime
import hashlib

class TimestampDocument(schema.Document):
    ts = schema.DateTimeField(default=datetime.now)

class Agent(TimestampDocument):
    type = schema.TextField(default='agent')
    post_ids = schema.ListField(schema.TextField())

class User(Agent):
    type = schema.TextField(default='user')
    username = schema.TextField()
    hashpass = schema.TextField()
    upvote_ids = schema.ListField(schema.TextField())
    influence = schema.DictField(schema.Schema.build())
#    influence = schema.DictField(schema.Schema.build(), default=int())
#     upvotes = schema.ListField(schema.DictField(schema.Schema.build(
#                 msg_id = schema.TextField(),
#                 ts = schema.DateTimeField()
#                 )))
    
class Message(TimestampDocument):
    agent_id = schema.TextField()
    title = schema.TextField()
    body = schema.TextField()
    link = schema.TextField()
    upvote_ids = schema.ListField(schema.TextField())
#     upvotes = schema.ListField(schema.DictField(schema.Schema.build(
#                 agent_id = schema.TextField(),
#                 ts = schema.DateTimeField()
#                 )))
    parent_id = schema.TextField()
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

def create_user(db, username, password):
    user = User(username=username, hashpass=hashlib.sha224(password).hexdigest())
    user.store(db)
    return user

def create_message(db, agent, title, link, body):
    msg = Message(agent_id=agent.id, title=title, body=body, link=link)
    msg.store(db)
    agent.post_ids.append(msg.id)
    agent.store(db)
    return msg

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

def respond_message(db, msg, agent, title, link, body):
    resp = create_message(db, agent, title, link, body)
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
# probably want separate dbs for agents, messages, and votes
# but not sure yet how that will affect views, etc.

class Test(unittest.TestCase):

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

    def test_create_message(self):
        user = create_user(self.db, 'name', 'password')
        assert user.id is not None
        msg = create_message(self.db, user, "Go here!", "http://...", "Wow, check it.")
        assert msg.id is not None
        self.assertEqual(msg.id, user.post_ids[0])

    def test_upvote_message(self):
        user = create_user(self.db, 'user', 'password')
        user2 = create_user(self.db, 'user2', 'password')
        msg = create_message(self.db, user, "Go here!", "http://...", "Wow, check it.")
        vote = upvote_message(self.db, user2, msg)
        assert vote.id is not None
        user = User.load(self.db, user.id) # ensure latest version
        self.assertEqual(1, user.influence[user2.id])

    def test_respond_message(self):
        user = create_user(self.db, 'user', 'password')
        user2 = create_user(self.db, 'user2', 'password')
        msg = create_message(self.db, user, "Go here!", "http://...", "Wow, check it.")
        resp = respond_message(self.db, msg, user2, "First Post!", None, "Blah, blah")
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
