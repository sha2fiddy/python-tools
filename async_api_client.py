from time import time
from typing import Type

from asyncio import TaskGroup, run  # TaskGroup object was added in Python 3.11
from httpx import AsyncClient, HTTPError, get

"""
# nest_asyncio is needed in for async code in some environments like Jupyter
# nest_asyncio requires full asyncio package, your mileage may vary
import asyncio
from nest_asyncio import apply as nest

nest()
"""

class API:
    """
    A base client for making API calls using httpx,
    and modern features of the asyncio library.
    Includes a call method for a single synchronous API call,
    and an async_calls method for multiple asyncronous API calls.
    """
    def __init__(self, url:str):
        """
        Requires API base url to initialize.
        
        param url: string, base url of API
        """
        self.url = url
        
    
    def call(self, endpoint:str, html:bool=False) -> str:
        """
        Method for making a single, synchronous API call.
        Takes an endpoint string and returns response text.
        
        param endpoint: string, API endpoint to follow base url
        param html: bool, default False, indicicates if HTML docstring responses are accepted
        """
        try:
            r = get(self.url + endpoint)
            r.raise_for_status()
            if not html and r.text[:15] == '<!doctype html>':
                print('Unexpetedly received HTML response: ' + self.url + endpoint)
                print(r.text)
                return None
            else:
                return r.text

        except HTTPError as e:
            raise SystemError(e)
        
    
    async def _async_call(self, client:Type[AsyncClient], endpoint:str, html:bool=False) -> str:
        """
        Hidden method for making a single API call asynchronously.
        Meant to be batched with the async_calls method.
        Takes an endpoint string and returns response text.
        
        param client, httpx AsyncClient object
        param endpoint: string, API endpoint to follow base url
        param html: bool, default False, indicicates if HTML docstring responses are accepted
        """
        try:
            r = await client.get(self.url + endpoint)
            r.raise_for_status()
            if not html and r.text[:15] == '<!doctype html>':
                print('Unexpetedly received HTML response: ' + self.url + endpoint)
                print(r.text)
                return None
            else:
                return r.text

        except HTTPError as e:
            raise SystemError(e)
        
        
    async def async_calls(self, endpoints:list) -> list:
        """
        Method for sending multiple API calls asynchronously.
        Takes a list of endpoint strings and returns a list of response texts.
        Requires the httpx AsyncClient object as the API client engine.
        The asyncio TaskGroup object was introduced in Python 3.11.
        
        param: endpoints: list, list of endpoint strings to follow base url
        """
        try:
            print(f'Making {len(endpoints)} calls to: ' + self.url + ' ...')
            start_time = time()
            async with AsyncClient() as client:
                async with TaskGroup() as tg:
                    tasks = [tg.create_task(self._async_call(client, e)) for e in endpoints]
                print('Completed in: %s seconds' % (time() - start_time) + '\n')
                return [t.result() for t in tasks]
        
        except:
            raise Exception('Error communicating with API')


"""
# Example usage:
url = 'https://mempool.space/api'
api = API(url)

blockheight = 750000
endpoint = f'/block-height/{blockheight}'
blockhash = api.call(endpoint)

endpoints = [
    f'/block/{blockhash}',
    f'/block/{blockhash}/status',
    f'/block/{blockhash}/txids'
]
responses = run(api.async_calls(endpoints))
"""
