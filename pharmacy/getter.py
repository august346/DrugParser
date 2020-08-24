import asyncio
import logging
from collections import namedtuple
from dataclasses import dataclass

from aiohttp import ClientSession, client_exceptions


async def drugs_batch_generator_queue_filling(queue: asyncio.Queue, **kwargs):
    batch_generator = Getter(**kwargs).drugs_batch_generator()
    async for batch in batch_generator:
        await queue.put(batch)
    await queue.put(None)


ResponseData = namedtuple('ResponseData', ('code', 'json'))


@dataclass
class Getter:
    pause_between_fetch: float
    src_max_page: int
    src_url: str
    request_query_fp: str

    logger_level = logging.INFO

    # to prepare
    request_query = None
    logger = None

    # temporary (may be useless)
    __session = None

    async def drugs_batch_generator(self):
        self._prepare()

        # self.session will be closed and useless after out of context!!!
        async with ClientSession() as self.__session:
            async for page_coroutine in self._pages_coroutine():
                yield (await page_coroutine)['data']['productDetail']['items']

    def _prepare(self):
        """Initializing object request_query and logger"""
        with open(self.request_query_fp, 'r', encoding='utf-8') as file:
            self.request_query = file.read()
        self.logger = logging.Logger('Getter', self.logger_level)

    async def _pages_coroutine(self):
        """src pages coroutine (contains 20 drags per page)"""
        for page in range(1, self.src_max_page):
            rsp_json = self._get_src_page(page)
            if rsp_json is not None:
                self.logger.info('{:2.2%}\t{}/{}'.format((page + 1) / self.src_max_page, page + 1, self.src_max_page))
                yield rsp_json

            await asyncio.sleep(self.pause_between_fetch)

    async def _get_src_page(self, page):
        """Getting response necessary data (status_code & json) from src"""
        try:
            rsp_data = await self._fetch_response_data(self._get_request_json(page))
            if rsp_data.code == 200:
                return rsp_data.json
        except (
                client_exceptions.ClientError,
                client_exceptions.ServerTimeoutError
        ) as e:
            self.logger.error(e)

    async def _fetch_response_data(self, request_json):
        """Fetching response from src"""
        async with self.__session.post(self.src_url, json=request_json) as resp:
            return ResponseData(resp.status, await resp.json())

    def _get_request_json(self, page):
        """Formatting request json"""
        return {
            'query': self.request_query,
            'variables': {'page': page}
        }
