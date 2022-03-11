#####################################################################
#                                                                   #
# /input_forwarder.py                                               #
#                                                                   #
# Copyright 2013, Monash University                                 #
#                                                                   #
# This file is part of the program BLACS, in the labscript suite    #
# (see http://labscriptsuite.org), and is licensed under the        #
# Simplified BSD License. See the license.txt file in the root of   #
# the project for the full license.                                 #
#                                                                   #
#####################################################################
from __future__ import division, unicode_literals, print_function, absolute_import

import traceback
from zprocess import Process
import zmq


class Forwarder(Process):
    def run(self, pub_port, sub_port):
        try:
            context = zmq.Context(1)

            # Socket facing clients
            frontend = context.socket(zmq.SUB)
            frontend.bind("tcp://*:{}".format(sub_port))
            frontend.setsockopt(zmq.SUBSCRIBE, "".encode('utf-8'))

            # Socket facing services
            backend = context.socket(zmq.PUB)
            backend.bind("tcp://*:{}".format(pub_port))
            zmq.device(zmq.FORWARDER, frontend, backend)
        except Exception:
            self.to_parent.put(traceback.format_exc())

    def __del__(self):
        self.terminate()
