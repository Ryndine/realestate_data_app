import re
import json
import httpx
from loguru import logger as log
from random import randint
from urllib.parse import urlencode
from parsel import Selector
from typing import List
import asyncio

class SearchZillow:
    async def zillow_request(query:str, session: httpx.AsyncClient, filters: dict=None, categories=("cat1", "cat2")):
        """base search function which is used by sale and rent search functions"""
        html_response = await session.get(f"https://www.zillow.com/homes/{query}_rb/")

        # find query data in search landing page
        query_data = json.loads(re.findall(r'"queryState":(\{.+}),\s*"filter', html_response.text)[0])
        if filters:
            query_data["filterState"] = filters

        # scrape search API
        url = "https://www.zillow.com/search/GetSearchPageState.htm?"
        found = []

        # cat1 - agent listings
        # cat2 - other listings
        for category in categories:
            full_query = {
                "searchQueryState": query_data,
                "wants": {category: ["mapResults"]},
                "requestId": randint(2, 10),
            }
            api_response = await session.get(url + urlencode(full_query))
            data = api_response.json()
            _total = data["categoryTotals"][category]["totalResultCount"]

            if _total > 500:
                log.warning(f"query has more results ({_total}) than 500 result limit ")
            else:
                log.info(f"found {_total} results for query: {query}")

            map_results = data[category]["searchResults"]["mapResults"]
            found.extend(map_results)
        return found

    @classmethod
    async def search_sale(self, query: str, session: httpx.AsyncClient):
        """search properties that are for sale"""
        log.info(f"scraping sale search for: {query}")
        return await self.zillow_request(query=query, session=session)

    @classmethod
    async def search_rent(self, query: str, session: httpx.AsyncClient):
        """search properites that are for rent"""
        log.info(f"scraping rent search for: {query}")

        filters = {
            "isForSaleForeclosure": {"value": False},
            "isMultiFamily": {"value": False},
            "isAllHomes": {"value": True},
            "isAuction": {"value": False},
            "isNewConstruction": {"value": False},
            "isForRent": {"value": True},
            "isLotLand": {"value": False},
            "isManufactured": {"value": False},
            "isForSaleByOwner": {"value": False},
            "isComingSoon": {"value": False},
            "isForSaleByAgent": {"value": False},
        }
        return await self.zillow_request(query=query, session=session, filters=filters, categories=["cat1"])

class ParseProperties:
    def parse_property(data: dict) -> dict:
        """parse zillow property"""
        # zillow property data is massive, let's take a look just
        # at the basic information to keep this tutorial brief:
        parsed = {
            "address": data["address"],
            "description": data["description"],
            "photos": [photo["url"] for photo in data["galleryPhotos"]],
            "zipcode": data["zipcode"],
            "phone": data["buildingPhoneNumber"],
            "name": data["buildingName"],
            # floor plans include price details, availability etc.
            "floor_plans": data["floorPlans"],
        }
        return parsed

    @classmethod
    async def scrape_properties(self, urls: List[str], session: httpx.AsyncClient):
        """scrape zillow properties"""
        async def scrape(url):
            resp = await session.get(url)
            sel = Selector(text=resp.text)
            data = sel.css("script#__NEXT_DATA__::text").get()
            if data:
                # some properties are located in NEXT DATA cache
                data = json.loads(data)
                return self.parse_property(data["props"]["initialReduxState"]["gdp"]["building"])
            else:
                # other times it's in Apollo cache
                data = sel.css('script#hdpApolloPreloadedData::text').get()
                data = json.loads(json.loads(data)['apiCache'])
                property_data = next(v['property'] for k, v in data.items() if 'ForSale' in k)
                return property_data

        return await asyncio.gather(*[scrape(url) for url in urls])

# the function used to initialize the search
async def find_listings(con_limit: int=5, timeout: float=15.0, headers: dict=None):
    limits = httpx.Limits(max_connections=con_limit)
    async with httpx.AsyncClient(limits=limits, timeout=httpx.Timeout(timeout), headers=headers) as session:
        data = await SearchZillow.search_rent("Honolulu, HI", session)
        data_json = json.dumps(data)
    return json.loads(data_json)

async def find_properties(url: str=None, con_limit: int=5, timeout: float=15.0, headers: dict=None):
    limits = httpx.Limits(max_connections=con_limit)
    async with httpx.AsyncClient(limits=limits, timeout=httpx.Timeout(timeout), headers=headers) as session:
        data = await ParseProperties.scrape_properties(
            [f"https://www.zillow.com{url}"], 
            session=session
        )
    return json.loads(data)
