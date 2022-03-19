import hashlib
import hmac
import logging
import time
import uuid
from urllib.parse import urlencode

import requests


class RestClient:

    def __init__(self, client_id, api_key, secret):
        self._logger = logging.getLogger(self.__class__.__name__)
        self.__clientID = client_id
        self.__apiKey = api_key
        self.__secret = secret

    def unauthenticated_get_request(self, url):
        response = requests.get('https://www.bitstamp.net/' + url)
        return response

    def request(self, url, payload):
        url = 'www.bitstamp.net/' + url
        timestamp = str(int(round(time.time() * 1000)))
        nonce = str(uuid.uuid4())
        content_type = 'application/x-www-form-urlencoded' if payload is not None else ''
        payload_string = urlencode(payload) if payload is not None else ''

        signature = self.create_signature(timestamp, nonce, content_type, url, payload_string)

        headers = {
            'X-Auth': 'BITSTAMP ' + self.__apiKey,
            'X-Auth-Signature': signature,
            'X-Auth-Nonce': nonce,
            'X-Auth-Timestamp': timestamp,
            'X-Auth-Version': 'v2',
        }
        if content_type != '':
            headers['Content-Type'] = content_type

        response = requests.post(
            'https://' + url,
            headers=headers,
            data=payload_string
        )

        self._logger.info('API request against Bitstamp')
        self._logger.info(f'URL: {url}')
        self._logger.info(f'Request: {payload_string}')
        self._logger.info(f'Response: {response.text}')

        if not response.ok:
            raise Exception("Response not ok: " + response.text)

        content = ''
        if response.content is not None:
            content = response.content
        string_to_sign = (nonce + timestamp + response.headers.get('Content-Type')).encode('utf-8') + content
        signature_check = hmac.new(self.__secret, msg=string_to_sign, digestmod=hashlib.sha256).hexdigest()
        if not response.headers.get('X-Server-Auth-Signature') == signature_check:
            raise Exception('Signatures do not match')

        return response

    def create_signature(self, timestamp, nonce, content_type, url, payload_string):
        message = 'BITSTAMP ' + self.__apiKey + \
                  'POST' + \
                  url + \
                  content_type + \
                  nonce + \
                  timestamp + \
                  'v2' + \
                  payload_string
        message = message.encode('utf-8')
        return hmac.new(self.__secret, msg=message, digestmod=hashlib.sha256).hexdigest()
