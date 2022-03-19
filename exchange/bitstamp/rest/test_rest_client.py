import hashlib
import hmac
import json
from unittest import TestCase

import requests_mock

from exchange.bitstamp.rest.rest_client import RestClient


def response_callback(request, context):
    string_to_sign = (request.headers.get('X-Auth-Nonce') +
                      request.headers.get('X-Auth-Timestamp') +
                      'application/json' + '{"some": "field"}').encode('utf-8')
    context.headers['Content-Type'] = 'application/json'
    context.headers['X-Server-Auth-Signature'] = hmac.new(b'a_secret', msg=string_to_sign,
                                                          digestmod=hashlib.sha256).hexdigest()
    return '{"some": "field"}'


def response_invalid_signature_callback(request, context):
    string_to_sign = (request.headers.get('X-Auth-Nonce') +
                      request.headers.get('X-Auth-Timestamp') +
                      'application/json' + '{"some": "field"}').encode('utf-8')
    context.headers['Content-Type'] = 'application/json'
    context.headers['X-Server-Auth-Signature'] = hmac.new(b'wrong_secret', msg=string_to_sign,
                                                          digestmod=hashlib.sha256).hexdigest()
    return '{"some": "field"}'


class RestClientTest(TestCase):

    def test_request(self):
        """It should create a correct request"""

        with requests_mock.Mocker() as m:
            client = RestClient('a_clientid', 'a_key', b'a_secret')
            url = 'someurl'
            payload = {'some': 'data', 'other': 'data'}
            m.post('https://www.bitstamp.net/' + url, text=response_callback)

            response = client.request(url, payload)

            self.assertEqual(1, m.call_count)
            self.assertEqual('field', json.loads(response.text)['some'])
            self.assertEqual(200, response.status_code)

            request = m.request_history[0]
            self.assertEqual('https://www.bitstamp.net/' + url, request.url)
            self.assertEqual('BITSTAMP a_key', request.headers['X-Auth'])
            self.assertEqual('v2', request.headers['X-Auth-Version'])
            self.assertEqual('application/x-www-form-urlencoded', request.headers['Content-Type'])
            self.assertEqual('POST', request.method)
            self.assertEqual('some=data&other=data', request.text)

    def test_signature(self):
        """It should generate a correct signature"""

        client = RestClient('a_clientid', 'a_key', b'a_secret')
        signature = client.create_signature('9877655', 'asdf', 'contenttype', 'www.bitstamp.net/some/url',
                                            'some=payload')
        self.assertEqual('10ff56949b1f9eb9d773edf808bd0a70020a1102db0fd53cdca28261083672a4', signature)

    def test_invalid_signature(self):
        """It should raise an exception if the server responds with an invalid signature"""

        with requests_mock.Mocker() as m:
            client = RestClient('a_clientid', 'a_key', b'a_secret')
            url = 'someurl'
            payload = {'some': 'data', 'other': 'data'}
            m.post('https://www.bitstamp.net/' + url, text=response_invalid_signature_callback)

            with self.assertRaises(Exception):
                client.request(url, payload)

    def test_unauthentiated_get_request(self):
        """It should create a correct request"""

        with requests_mock.Mocker() as m:
            client = RestClient('a_clientid', 'a_key', b'a_secret')
            url = 'someurl'
            m.get('https://www.bitstamp.net/' + url, text='hello world!')

            response = client.unauthenticated_get_request('someurl')

            self.assertEqual(200, response.status_code)
            self.assertEqual('hello world!', response.text)

            request = m.request_history[0]
            self.assertEqual('GET', request.method)
            self.assertEqual('https://www.bitstamp.net/' + url, request.url)
            self.assertFalse('X-Auth' in request.headers)
            self.assertFalse('X-Auth-Version' in  request.headers)
            self.assertFalse('Content-Type' in request.headers)

