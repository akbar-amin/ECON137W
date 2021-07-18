import json
import time
import random
import logging
import itertools
import pandas as pd
import urllib.parse
import urllib.error
import urllib.request
from collections import defaultdict

LOG = logging.getLogger('RedditCount')
API = 'https://api.pushshift.io/reddit/search/comment'


class RedditCount:
    def __init__(self, dt1, dt2, freq='1D'):
        self._dt1 = pd.to_datetime(dt1)
        self._dt2 = pd.to_datetime(dt2)
        self._offset = pd.tseries.frequencies.to_offset(freq)
        self._counts = defaultdict(list)

    @staticmethod
    def intervals(index):
        start, stop = itertools.tee(index)
        next(stop, None)
        for dt1, dt2 in zip(start, stop):
            yield (dt1, dt2)

    def get(self, query, subreddit='*'):
        for series in self._counts[subreddit]:
            if series.name == query:
                return series
        raise KeyError(query, subreddit)

    def request(self, dt1, dt2, query, subreddit=None):
        params = dict(
            q=query,
            size=0,
            sort='desc',
            after=dt1,
            before=dt2,
            metadata=True,
            subreddit=subreddit,
        )
        params = {k: str(v) for k, v in params.items() if v not in (None, '')}
        request = API + '?' + urllib.parse.urlencode(params)
        content = None
        while content is None:
            try:
                with urllib.request.urlopen(request, timeout=None) as response:
                    content = json.loads(response.read().decode('UTF-8'))
            except urllib.error.HTTPError as e:
                LOG.error(e)
                if e.code >= 500:
                    break
                time.sleep(random.uniform(0, 6))
        time.sleep(0.25 + random.random())
        return content

    def search(self, query, subreddit=None, outfile=None):
        index = pd.date_range(self._dt1, self._dt2, freq=self._offset)
        count = pd.Series(dtype=int, index=index, name=query)[1:]
        for dt1, dt2 in self.intervals(index):
            ts1, ts2 = dt1.timestamp(), dt2.timestamp()
            content = self.request(int(ts1), int(ts2), query, subreddit)
            if not content:
                LOG.info('%s: %d' % (dt2, 0))
                continue
            total = content['metadata']['total_results']
            count.at[dt2] = total
            LOG.info('%s: %d' % (dt2, total))
        self._counts[subreddit or '*'].append(count)
        if outfile:
            count.to_csv(outfile)
        LOG.info(count)
        return count
