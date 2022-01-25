import time
import random
import requests
import itertools
import pandas as pd

API = 'https://api.pushshift.io/reddit/search/comment'

def intervals(index: pd.DatetimeIndex):
    it1, it2 = itertools.tee(index)
    next(it2, None)
    yield from zip(it1, it2)

def request(t1: pd.Timestamp, t2: pd.Timestamp, **kwds):
    t1, t2 = int(t1.timestamp()), int(t2.timestamp())
    kwds.update(size=0, metadata=True, after=t1, before=t2)
    count = None
    with requests.Session() as s:
        s.params = kwds
        while count is None:
            r = s.get(API, timeout=None)
            if r.status_code == 200:
                json = r.json()
                if isinstance(json, dict) \
                        and 'metadata' in json:
                    count = json['metadata']['total_results']
                else:
                    count = 0
            elif r.status_code >= 500:
                count = 0
            else:
                time.sleep(random.uniform(0, 1))
    return count


def search(dt1, dt2, term, subr=None, freq=None, name=None, **kwds):
    kwds.update(q=term, subreddit=subr)
    index = pd.date_range(dt1, dt2, freq=freq)
    index = index.union([index[0] - (1 * index.freq)])
    name = name or '%s_%s' % (subr or 'reddit', term)
    data = map(lambda it: request(*it, **kwds), intervals(index))
    return pd.Series(data, index=index[1:], dtype=int, name=name)


if __name__ == '__main__':
    wsb = search('2021-07-29', '2021-07-30', 'GME', 'wallstreetbets')
