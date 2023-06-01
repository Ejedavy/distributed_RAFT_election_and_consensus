import random
import sched
import socket
import time
from threading import Thread
from argparse import ArgumentParser
from enum import Enum
from xmlrpc.client import ServerProxy
from xmlrpc.server import SimpleXMLRPCServer

PORT = 1234
CLUSTER = [1, 2, 3]
ELECTION_TIMEOUT = (6, 8)
HEARTBEAT_INTERVAL = 5


class NodeState(Enum):
    """Enumerates the three possible node states (follower, candidate, or leader)"""
    FOLLOWER = 1
    CANDIDATE = 2
    LEADER = 3


class Node:
    def __init__(self, node_id):
        """Non-blocking procedure to initialize all node parameters and start the first election timer"""
        self.node_id = node_id
        self.state = NodeState.FOLLOWER
        self.term = 0
        self.votes = {}
        self.log = []
        self.other_nodes = [x for x in range(1, 4) if x != node_id]
        self.pending_entry = ''
        self.sched = sched.scheduler()
        self.ELECTION_DURATION = random.randint(ELECTION_TIMEOUT[0], ELECTION_TIMEOUT[1])
        # TODO: start election timer for this node
        self.election_event = self.sched.enter(self.ELECTION_DURATION, priority=0, action= self.hold_election)
        print(f"Node started! State: {self.state}. Term: {self.term}")

    
    def is_port_open(self, host, port):
        """
        Check if a TCP port is open on a remote host.
        :param host: The hostname or IP address of the remote host.
        :param port: The port number to check.
        :return: True if the port is open, False otherwise.
        """
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect((host, port))
            s.shutdown(socket.SHUT_RDWR)
            return True
        except:
            return False
        finally:
            s.close()

    def is_leader(self):
        """Returns True if this node is the elected cluster leader and False otherwise"""
        return self.state == NodeState.LEADER


    def reset_election_timer(self):
        """Resets election timer for this (follower or candidate) node and returns it to the follower state"""
        try:
            self.sched.cancel(self.election_event)
        except:
            pass
        finally:
            self.ELECTION_DURATION = random.randint(ELECTION_TIMEOUT[0], ELECTION_TIMEOUT[1])
            self.election_event = self.sched.enter(self.ELECTION_DURATION, priority=0, action= self.hold_election)
        self.state = NodeState.FOLLOWER
        return

    def hold_election(self):
        """Called when this follower node is done waiting for a message from a leader (election timeout)
            The node increments term number, becomes a candidate and votes for itself.
            Then call request_vote over RPC for all other online nodes and collects their votes.
            If the node gets the majority of votes, it becomes a leader and starts the hearbeat timer
            If the node loses the election, it returns to the follower state and resets election timer.
        """
        self.term += 1
        self.state = NodeState.CANDIDATE
        print(f"Starting election. Node {self.node_id} is now a candidate")
        self.votes[self.term] = [self.node_id]
        response = []
        for i in self.other_nodes:
            print(f"Requesting vote from {i}")
            if not self.is_port_open(f"node_{i}", PORT):
                print(f"Node {i} is dead")
                continue
            o_node = ServerProxy(f'http://node_{i}:{PORT}')
            resp = o_node.request_vote(self.term, self.node_id)
            response.append((i, resp))
        for n, r in response:
            if r:
                self.votes[self.term].append(n)
        if len(self.votes[self.term]) >= 2:
            self.state = NodeState.LEADER
            self.heartbeat_event = self.sched.enter(HEARTBEAT_INTERVAL, priority=0, action= self.append_entries)
        else:
            self.reset_election_timer()
        print(f"New election term {self.term}. State: {self.state}")
        return

    def request_vote(self, term, candidate_id):
        """ Called remotely when a node requests voting from other nodes.
            Updates the term number if the received one is greater than `self.term`
            A node rejects the vote request if it's a leader or it already voted in this term.
            Returns True and update `self.votes` if the vote is granted to the requester candidate and False otherwise.
        """
        if term < self.term:
            return False
        else:
            self.term = term
        if self.is_leader() or not self.votes.get(term) is None:
            print(f"Did not vote for {candidate_id} already voted for {self.votes[term][0]}")
            return False
        self.votes[term] = [candidate_id]
        print(f"Got a vote request from {candidate_id} (term={term})")
        self.reset_election_timer()
        return True

    def append_entries(self):
        """Called by leader every HEARTBEAT_INTERVAL, sends a heartbeat message over RPC to all online followers.
            Accumulates ACKs from followers for a pending log entry (if any)
            If the majority of followers ACKed the entry, the entry is committed to the log and is no longer pending
        """
        print("Sending a heartbeat to followers")
        responses = []
        for i in self.other_nodes:
            if not self.is_port_open(f"node_{i}", PORT):
                print(f"Node {i} is dead")
                continue
            n = ServerProxy(f'http://node_{i}:{PORT}')
            a = n.heartbeat(f"{self.term}@{self.pending_entry}")
            if a:
                responses.append(a)
        if len(responses) >= 2:
            if self.pending_entry != "":
                self.log.append(self.pending_entry)
                self.pending_entry = ""
                print(f"Leader committed '{self.log[-1]}'")
        self.heartbeat_event = self.sched.enter(HEARTBEAT_INTERVAL, priority=0, action= self.append_entries)
        return

    def heartbeat(self, leader_entry):
        term = int(leader_entry.split("@")[0])
        leader_entry = leader_entry.split("@")[1]
        print(f"Heartbeat received from leader (entry='{leader_entry}')")
        if self.term <= term:
            self.term = term
            self.reset_election_timer()
        else:
            return False
        """Called remotely from the leader to inform followers that it's alive and supply any pending log entry
            Followers would commit an entry if it was pending before, but is no longer now.
            Returns True to ACK the heartbeat and False on any problems.
        """
        if self.pending_entry != "":
            self.log.append(self.pending_entry)
            self.pending_entry = ""
        elif self.pending_entry == "" and leader_entry != "":
            self.pending_entry = leader_entry
        else:
            pass
        return True

    def leader_receive_log(self, log):
        """Called remotely from the client. Executed only by the leader upon receiving a new log entry
            Returns True after the entry is committed to the leader log and False on any problems
        """
        if self.pending_entry != "":
            return False
        self.pending_entry = log
        print(f"Leader received log '{log}' from client")
        time.sleep(5)
        return True



if __name__ == '__main__':
    parser = ArgumentParser(description='Start a Raft node')
    parser.add_argument('node_id', type=int, help='ID of this node')
    args = parser.parse_args()

    node = Node(args.node_id)

    server = SimpleXMLRPCServer(('0.0.0.0', PORT), allow_none=True, logRequests=False)
    server.register_instance(node)
    print(f"Node server listening on 0.0.0.0:{PORT}")

    thread = Thread(target=node.sched.run)
    thread.start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("KeyboardInterrupt detected, shutting down gracefully...")
        server.shutdown()
        thread.join()