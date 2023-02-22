from time import time
from json import loads
from typing import Type

from asyncio import TaskGroup, run
from httpx import AsyncClient, HTTPError, get
