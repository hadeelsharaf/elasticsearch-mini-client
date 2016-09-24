from urllib import urlencode
import requests
import json
import time
import logging


logger = logging.getLogger('elasticsearch')


class Elasticsearch(object):
    mimetype = 'application/json'

    def __init__(self, host=None, timeout=30, max_retries=1, retry_on_timeout=False):
        self.host = host
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_on_timeout = retry_on_timeout

    def _get_func(self, call):
        calls = {
            'post': requests.post,
            'put': requests.put,
            'delete': requests.delete,
            'get': requests.get
        }
        return calls.get(call, 'get')

    def _build_url(self, args=None):
        """args are supposed to contain ['index','doctype' ,'id'] to be appended to the url
        and also opreations like _search and _bulk
        """
        url = ''
        if '://' in self.host:
            url = self.host
        else:
            url = 'http://%s:%s/' % (self.host.get('host'), self.host.get('port'))
        if args:
            return '%s%s' % (url, '/'.join(args))
        return url

    def _make_request(self, method, index=None, doctype=None,
                      id=None, params=None, body=None, extra_op=None):
        args = []
        if index:
            args.append(index)
        if doctype:
            args.append(doctype)
        if id:
            args.append(id)
        if extra_op:
            args.append(extra_op)

        url = self._build_url(args)
        if params:
            url = '%s?%s' % (url, urlencode(params))
        data = body
        if body and not isinstance(body, str):
            data = json.dumps(body)
        start = time.time()
        try:
            res = self._get_func(method)(url, data=data, headers={"content-type": self.mimetype})
            duration = time.time() - start
            if 200 < res.status_code or res.status_code > 300:
                self.log_request_fail(method, url, body, duration, res.status_code)
            return res.json()
        except requests.Timeout:
            duration = time.time() - start
            print 'url: %s reached the timeout limit' % url
            self.log_request_fail(method, url, body, duration, 'timeout')

    def index(self, index, body, doc_type=None, id=None, params=None):
        method = 'put' if id else 'post'
        return self._make_request(method, index=index, body=body, doctype=doc_type, id=id, params=params)

    def search(self, index=None, doc_type=None, body=None, params=None, **kwargs):
        params = params or {}
        if 'from_' in params:
            params['from'] = params.pop('from_')
        else:
            params['from'] = params.get('from', 0)

        if 'ignore_unavailable' not in params:
            params['ignore_unavailable'] = True
        if kwargs and 'size' in kwargs:
            params['size'] = kwargs.pop('size')
        else:
            params['size'] = params.get('size', 20)
        if doc_type and not index:
            index = '_all'
        if body and isinstance(body, str):
            body = json.loads(body)
        body = body or {}
        body.update(kwargs)
        return self._make_request('get', index=index, doctype=doc_type, params=params, body=body, extra_op='_search')

    def bulk(self, body, index=None, doc_type=None, params=None, **kwargs):
        if kwargs and 'refresh' in kwargs:
            params = params or {}
            params['refresh'] = kwargs.pop('refresh')
        if isinstance(body, list):
            body = '\n'.join(body)
        return self._make_request('post', index=index, body=body, doctype=doc_type, params=params, extra_op='_bulk')

    def count(self, body=None, index=None, doc_type=None, params=None, **kwargs):
        return self._make_request('post', index=index, body=body, doctype=doc_type, params=params, extra_op='_count')

    def delete(self, index, doc_type=None, id=None, params=None, **kwargs):
        return self._make_request('delete', index=index, doctype=doc_type, id=id, params=params)

    def log_request_fail(self, method, full_url, body, duration, status_code=None):
        """ Log an unsuccessful API call.  """
        logger.warning(
            '%s %s [status:%s request time:%.3fs]', method, full_url,
            status_code or 'N/A', duration
        )
        if body:
            logger.info('> %s', body)
