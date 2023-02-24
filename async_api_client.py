from time import time
from typing import Type

from asyncio import TaskGroup, run  # TaskGroup object was added in Python 3.11
from httpx import AsyncClient, HTTPError, get

"""
# nest_asyncio is needed for async code in some environments like Jupyter
# nest_asyncio requires full asyncio package, your mileage may vary
import asyncio
from nest_asyncio import apply as nest
nest()
"""

class API:
    """
    A base client for making API calls using httpx
    and modern features of the asyncio library.
    Includes a call method for a single synchronous API call,
    an async_calls method for multiple asyncronous API calls,
    and an async_calls_wrapper method to use async_calls synchronously.
    """
    def __init__(self, url:str):
        """
        Requires API base url to initialize.
        
        param url: string, base url of API
        """
        self.url = url
        
    
    def call(self, endpoint:str, params:dict=None, html:bool=False) -> str:
        """
        Method for making a single, synchronous API call.
        Takes an endpoint string and returns response text.
        
        param endpoint: string, API endpoint to follow base url
        param params: dict, default None, optional params object to pass to the API
        param html: bool, default False, indicicates if HTML docstring responses are accepted
        """
        try:
            r = get(self.url + endpoint, params=params)
            r.raise_for_status()
            if not html and r.text[:15] == '<!doctype html>':
                print('Unexpetedly received HTML response: ' + self.url + endpoint)
                print(r.text[:10000] + '\n' + '...')
                return None
            else:
                return r.text

        except HTTPError as e:
            raise SystemError(e)
        
    
    async def _async_call(self, client:Type[AsyncClient], endpoint:str,
                          params:dict=None, html:bool=False) -> str:
        """
        Hidden method for making a single API call asynchronously.
        Meant to be batched with the async_calls method.
        Takes an endpoint string and returns response text.
        
        param client, httpx AsyncClient object
        param endpoint: string, API endpoint to follow base url
        param params: dict, default None, optional params object to pass to the API
        param html: bool, default False, indicicates if HTML docstring responses are accepted
        """
        try:
            r = await client.get(self.url + endpoint, params=params)
            r.raise_for_status()
            if not html and r.text[:15] == '<!doctype html>':
                print('Unexpetedly received HTML response: ' + self.url + endpoint)
                print(r.text[:10000] + '\n' + '...')
                return None
            else:
                return r.text

        except HTTPError as e:
            raise SystemError(e)
        
        
    async def async_calls(self, endpoints:list, params:dict=None, html:bool=False) -> list:
        """
        Method for sending multiple API calls asynchronously.
        Takes a list of endpoint strings and returns a list of response texts.
        Requires the httpx AsyncClient object as the API client engine.
        The asyncio TaskGroup object was introduced in Python 3.11.
        
        param: endpoints: list, list of endpoint strings to follow base url
        param params: dict, default None, optional params object to pass to the API
        param html: bool, default False, indicicates if HTML docstring responses are accepted
        """
        try:
            print(f'Making {len(endpoints)} calls to: ' + self.url + ' ...')
            start_time = time()
            async with AsyncClient() as client:
                async with TaskGroup() as tg:
                    tasks = [tg.create_task(self._async_call(client, e, params, html)) for e in endpoints]
                print('Completed in: %s seconds' % (time() - start_time) + '\n')
                return [t.result() for t in tasks]
        
        except:
            raise Exception('Error communicating with API')
    
    
    def async_calls_wrapper(self, endpoints:list, params:dict=None, html:bool=False) -> list:
        """
        Wrapper method for executing the async_calls
        method without importing asyncio.run.
        Takes a list of endpoint strings and returns a list of response texts.
        
        param: endpoints: list, list of endpoint strings to follow base url
        param params: dict, default None, optional params object to pass to the API
        param html: bool, default False, indicicates if HTML docstring responses are accepted
        """
        try:
            return run(self.async_calls(endpoints, params, html))
        
        except:
            raise Exception('Error executing async_calls method')



"""
# Example usage:
url = 'https://mempool.space/api'
api = API(url)

blockheight = 777777
endpoint = f'/block-height/{blockheight}'
blockhash = api.call(endpoint)

endpoints = [
    f'/block/{blockhash}',
    f'/block/{blockhash}/status',
    f'/block/{blockhash}/txids'
]
responses = api.async_calls_wrapper(endpoints)

# OR
from asyncio import run
responses = run(api.async_calls(endpoints))
"""
