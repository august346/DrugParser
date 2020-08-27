import asyncio
import os

from pharmacy import utils
from pharmacy.getter import drugs_batch_generator_queue_filling
from pharmacy.saver import pharmacy_inserting


SRC_MAX_PAGE = 626
SRC_PAUSE_BETWEEN_FETCH = 2
SRC_URL_MASKED = '68747470733a2f2f7777772e7269676c612e72752f6772617068716c'
SRC_REQUEST_QUERY_FP = os.path.join(os.getcwd(), 'src/o3_request_query.txt')


async def main():
    src_data_queue = asyncio.Queue()

    filling_queue_task = asyncio.create_task(
        drugs_batch_generator_queue_filling(
            src_data_queue,
            pause_between_fetch=SRC_PAUSE_BETWEEN_FETCH,
            request_query_fp=SRC_REQUEST_QUERY_FP,
            src_max_page=SRC_MAX_PAGE,
            src_url=utils.decode(SRC_URL_MASKED, 'hex')
        )
    )

    insert_data_flow = asyncio.create_task(
        pharmacy_inserting(
            src_data_queue,
            'o3'
        )
    )

    await asyncio.gather(
        filling_queue_task,
        insert_data_flow,
        return_exceptions=True
    )

if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
