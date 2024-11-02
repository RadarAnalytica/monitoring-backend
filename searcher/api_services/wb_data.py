import asyncio

from aiohttp import ClientSession


async def get_today_product_list_data(product_wb_id_list: list[int], rqa=3):
    product_wb_id_list = sorted(product_wb_id_list)
    result_status = 0
    count = 0
    async with ClientSession() as http_session:
        while result_status != 200 or count >= rqa:
            count += 1
            try:
                async with http_session.get('https://card.wb.ru/cards/v2/detail', params={
                    'appType': 1,
                    'curr': 'rub',
                    'dest': -1257786,
                    'spp': 30,
                    'nm': ";".join([str(p_id) for p_id in product_wb_id_list])
                }) as resp:
                    result_status = resp.status
                    if result_status != 200:
                        continue
                    _data = await resp.json()
                    products = _data.get('data').get('products')
                    print([product.get("id") for product in products])
                    return


            except Exception as error:
                print(f'Error on products {product_wb_id_list}: {result_status} ::: (Error: {error}) sleep: {count * 0.2}')
                _data = ''
                result_status = 0
                await asyncio.sleep(count * 0.2)
                count += 1

left = 900001
right = 900301
for _ in range(10):
    asyncio.run(get_today_product_list_data([i for i in range(left, right)]))
    left += 300
    right += 300