from contextlib import contextmanager
from collections import *

class Context(object):
    
    def __init__(self):
        self.lines = []

    def add_line(self, line, **formatargs):
        if formatargs:
            line = line.format(**formatargs)
        self.lines.append(line)

    def add_lines(self, lines, **formatargs):
        if formatargs:
            lines = [line.format(**formatargs) for line in lines]

        for line in lines:
            self.lines.append(line)
