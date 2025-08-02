import asyncio
import math
from datetime import date, datetime

from aiohttp import ClientSession


async def get_supplier_data(supplier_id: int, http_session: ClientSession):
    if not supplier_id:
        return dict()
    url = f"https://suppliers-shipment-2.wildberries.ru/api/v1/suppliers/{supplier_id}"
    headers = {"X-Client-Name": "site"}
    result_status = 0
    count = 0
    result = dict()
    try:
        while result_status != 200 and count < 2:
            async with http_session.get(url, timeout=5, headers=headers) as resp:
                result_status = resp.status
                if result_status == 404:
                    return result
                result = await resp.json()
            return result
    except Exception as error:
        print(
            f"Error on supplier_id {supplier_id}: {result_status} ::: (Error: {error}) sleep: {count * 0.2}"
        )
        await asyncio.sleep(count * 0.2)
        count += 1
    return result


async def get_supplier_metadata(supplier_id: int, http_session: ClientSession):
    if not supplier_id:
        return dict()
    url = f"https://static-basket-01.wbcontent.net/vol0/data/supplier-by-id/{supplier_id}.json"
    result_status = 0
    count = 0
    result = dict()
    try:
        while result_status != 200 and count < 2:
            async with http_session.get(url, timeout=5) as resp:
                result_status = resp.status
                if result_status == 404:
                    return result
                result = await resp.json()
            return result
    except Exception as error:
        print(
            f"Error on supplier_id {supplier_id}: {result_status} ::: (Error: {error}) sleep: {count * 0.2}"
        )
        await asyncio.sleep(count * 0.2)
        count += 1
    return result


async def get_supplier_full(supplier_id: int, http_session: ClientSession, date_: date):
    sd_task = asyncio.create_task(get_supplier_data(supplier_id, http_session))
    sdm_task = asyncio.create_task(get_supplier_metadata(supplier_id, http_session))

    sd, sdm = await asyncio.gather(sd_task, sdm_task)
    if not sd or not sdm:
        return [], []
    name = str(sdm.get("supplierName", ""))
    full_name = str(sdm.get("supplierFullName", ""))
    inn = str(sdm.get("inn", ""))
    ogrn = str(sdm.get("ogrn", ""))
    ogrnip = str(sdm.get("ogrnip", ""))
    trademark = str(sdm.get("trademark", ""))
    kpp = str(sdm.get("kpp", ""))
    unp = str(sdm.get("unp", ""))
    bin_ = str(sdm.get("bin", ""))
    unn = str(sdm.get("unn", ""))
    taxpayer_code = str(sdm.get("taxpayerCode", ""))
    rating = sd.get("rating", 0)
    try:
        valuation = float(sd.get("valuation", 0))
    except (ValueError, TypeError):
        valuation = 0
    feedbacks_count = sd.get("feedbacksCount", 0)
    registration_date = sd.get("registrationDate", None)
    if registration_date:
        registration_date = datetime.fromisoformat(registration_date)
    else:
        return [], []
    sold_item_quantity = sd.get("saleItemQuantity", 0) or 0
    supp_ratio = sd.get("suppRatio", 0) or 0

    meta_data = [
        (
            supplier_id,  #
            name,
            full_name,
            trademark,
            inn,
            taxpayer_code,
            registration_date,
            ogrn,
            ogrnip,
            kpp,
            unn,
            unp,
            bin_,
        )
    ]

    history_data = [
        (
            supplier_id,  #
            date_,  #
            0 if math.isnan(rating) else rating,  #
            0 if math.isnan(valuation) else valuation,
            0 if math.isnan(feedbacks_count) else feedbacks_count,  #
            0 if math.isnan(supp_ratio) else supp_ratio,  #
            0 if math.isnan(sold_item_quantity) else sold_item_quantity,  #
        )
    ]
    print(f"данные supplier {supplier_id} получены")
    return meta_data, history_data


async def http_worker_supplier(
    main_queue: asyncio.Queue,
    meta_queue: asyncio.Queue,
    history_queue: asyncio.Queue,
    date_: date,
):
    async with ClientSession() as http_session:
        while True:
            supplier_id = await main_queue.get()
            if supplier_id is None:
                await main_queue.put(None)
                return
            supplier_meta, supplier_history = await get_supplier_full(
                supplier_id=supplier_id, http_session=http_session, date_=date_
            )
            if supplier_meta:
                await meta_queue.put(supplier_meta)
            if supplier_history:
                await history_queue.put(supplier_history)
            print("цикл воркера + ")
