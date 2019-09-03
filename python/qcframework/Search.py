""" Nodule to search for regex in text strings

"""

import re
import math
import copy
import datetime

class Search(object):
    """ Class to compare a string to values in the MESSAGE_PATTERN table and return which pattern
        was matched. Any string which matches any entry in the MESSAGE_IGNORE table is disregarded,
        regardless of any matches from MESSAGE_PATTERN.

        Keywords
        --------
        patterns : dict
            Dictionary containing the patterns from MESSAGE_PATTERN and associated data

        exclude : list
            List of patterns from MESSAGE_IGNORE.
            Default: []

    """
    def __init__(self, patterns, exclude=[]):
        self._maxlen = 99            # maximum number of patterns in each compiled regex, this can be no longer than 99
        self.exclude = exclude
        self.patterns = copy.deepcopy(patterns)
        # reverse the patterns so the indexing is correct
        self.patterns.reverse()
        self._exclude = []
        self._patterns = []

        self._lenex = int(math.ceil(len(exclude)/float(self._maxlen)))
        self._lenpat = int(math.ceil(len(patterns)/float(self._maxlen)))
        # precompile the regex's to make searching faster
        self._exclude = [re.compile('(' + ')|('.join(exclude[i * self._maxlen: (i * self._maxlen) + \
                         self._maxlen]) + ')', re.IGNORECASE|re.DOTALL) for i in range(self._lenex)]
        self._patterns = [re.compile('(' + ')|('.join(self.patterns[i * self._maxlen: (i * self._maxlen) + \
                         self._maxlen]) + ')', re.IGNORECASE|re.DOTALL) for i in range(self._lenpat)]
        self._patterns.reverse()
        # set up possible date stamp formats, used to look for dates in the strings, which are preferred
        # to the auto generated ones as they are more accurate
        self._timepat = re.compile(r'(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\.?\d*)')
        self._timepat2 = re.compile(r'(\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2}\.?\d*)')

    def search(self, string):
        """ Method to preform the searches

            Parameters
            ----------
            string : str
                The string to search for patterns in

            Returns
            -------
            Tuple containing the index of the first match and the matched text
        """
        # search for any matches from MESSAGE_IGNORE, if any are found then ignore the string and return
        for exp in self._exclude:
            if exp.search(string):
                return (None, '')
        # search for any matches from MESSAGE_PATTERN
        for i, exp in enumerate(self._patterns):
            match = exp.search(string)
            if match:
                return (len(self.patterns) - match.lastindex + (i * self._maxlen), match.group(match.lastindex))
            else:
                return (None, '')

    def findtime(self, string):
        """ Method to search the given string for any date/time stamps

            Parameters
            ----------
            string : str
                String containing the text to search

            Returns
            -------
            String containing either the found date/time or the current time if none were found

        """
        match = self._timepat.search(string)
        if match:
            return match.group(1)
        match = self._timepat2.search(string)
        if match:
            return match.group(1)
        return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
