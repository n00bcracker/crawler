from yargy import Parser
from natasha.tokenizer import TOKENIZER
from natasha.preprocess import normalize_text
from natasha.extractors import Matches
from time import time

class CapError(Exception):
    pass


class TooManyStatesError(CapError):
    pass


class TimeoutError(CapError):
    pass


try:
    RecursionError
except NameError:
    # for pypy
    class RecursionError:
        pass

def capped(method):
    def wrap(self, column, *args):
        before = len(column.states)
        method(self, column, *args)
        after = len(column.states)

        self.states += (after - before)
        if self.cap and self.states > self.cap:
            raise TooManyStatesError

        current = time()
        self.duration = current - self.start
        if self.timeout and self.duration > self.timeout:
            raise TimeoutError

    return wrap

class CappedParser(Parser):


    def __init__(self, *args, **kwargs):
        self.cap = kwargs.pop('cap', None)
        self.timeout = kwargs.pop('timeout', None)
        self.reset()
        Parser.__init__(self, *args, **kwargs)

    predict = capped(Parser.predict)
    scan = capped(Parser.scan)
    complete = capped(Parser.complete)

    def reset(self):
        self.states = 0
        self.start = time()
        self.duration = 0

    def chart(self, *args, **kwargs):
        self.reset()
        return Parser.chart(self, *args, **kwargs)

    def safe_findall(self, text):
        try:
            return self.findall(text)
        except (CapError, RecursionError):
            return []


class CappedExtractor(object):
    def __init__(self, rule, tokenizer=TOKENIZER, tagger=None):
        self.parser = CappedParser(rule, tokenizer=tokenizer, tagger=tagger, timeout=5)

    def __call__(self, text):
        text = normalize_text(text)
        matches = self.parser.safe_findall(text)
        return Matches(text, matches)