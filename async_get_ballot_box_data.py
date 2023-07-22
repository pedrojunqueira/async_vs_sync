import asyncio
from time import perf_counter
import json
from pathlib import Path

import aiohttp
import aiofiles

BASE_DIR = Path(".").resolve()

bu_path = BASE_DIR / "ballot_files"

STATE_BALLOTS_FILE_PATH = "./state_ballots/{uf}-p000407-cs.json"
URL_HASH = "https://resultados.tse.jus.br/oficial/ele2022/arquivo-urna/407/dados/{uf}/{mu_cd}/{zone_cd}/{section_cd}/p000407-{uf}-m{mu_cd}-z{zone_cd}-s{section_cd}-aux.json"
URL_BU = "https://resultados.tse.jus.br/oficial/ele2022/arquivo-urna/407/dados/{uf}/{mu_cd}/{zone_cd}/{section_cd}/{hash}/o00407-{mu_cd}{zone_cd}{section_cd}.bu"

# each ballot has the following structure:
# {'state_code': 'ap', 'mu_cd': '06122', 'zone_cd': '0007', 'section_cd': '0181'

def get_state_ballot_codes(state_code:str)-> list:
    ballots = []

    with open(STATE_BALLOTS_FILE_PATH.format(uf=state_code.lower()), "r+") as fp:
        state_data = json.load(fp)

    state_code = state_data["abr"][0]["cd"]
    councils  = state_data["abr"][0]["mu"]

    for council in councils:
        mu_cd = council["cd"]
        zones = council["zon"]
        for zone in zones:
            zone_cd = zone["cd"]
            sections = zone["sec"]
            for section in sections:
                section_cd = section["ns"]
                ballots.append(dict(state_code=state_code.lower(), mu_cd=mu_cd, zone_cd=zone_cd, section_cd=section_cd))
    
    return ballots

async def fetch_hash(ballot_code:dict, session):
    
    url_request = URL_HASH.format(uf=ballot_code["state_code"], mu_cd=ballot_code["mu_cd"], 
                                zone_cd=ballot_code["zone_cd"],section_cd=ballot_code["section_cd"])

    async with session.get(url_request) as r:
        if r.status != 200:
            r.raise_for_status()
        text = await r.text()
        hash_data  = json.loads(text)
        hash_code = hash_data["hashes"][0]["hash"]
        return (ballot_code, hash_code)

async def fetch_bb(ballot_code:dict,hash_code:str,session):
    url_request = URL_BU.format(uf=ballot_code["state_code"], mu_cd=ballot_code["mu_cd"], 
                                    zone_cd=ballot_code["zone_cd"],section_cd=ballot_code["section_cd"],hash=hash_code)
    async with session.get(url_request) as r:
        if r.status != 200:
            r.raise_for_status()
        content = await r.content.read()
        return (ballot_code,content)
    
def make_bu_file_name(ballot_codes:dict)-> str:
    mu_cd = ballot_codes['mu_cd']
    zone_cd = ballot_codes['zone_cd']
    section_cd = ballot_codes['section_cd']
    return f"o00407-{mu_cd}{zone_cd}{section_cd}.bu"

async def save_file(filename, content):
    async with aiofiles.open(bu_path / filename, "wb") as fp:
        await fp.write(content)
        await fp.flush()
    return f"{filename} successfully saved"

async def gather_hash_tasks(ballot_codes:dict, session: aiohttp.ClientSession):
    tasks = []
    for ballot_code in ballot_codes:
        task = asyncio.create_task(fetch_hash(ballot_code, session))
        tasks.append(task)
    future_tasks = await asyncio.gather(*tasks)
    return future_tasks

async def gather_bb_tasks(batch_input, session):
    tasks = []
    for ballot_code, hash_code in batch_input:
        task = asyncio.create_task(fetch_bb(ballot_code, hash_code, session))
        tasks.append(task)
    future_tasks = await asyncio.gather(*tasks)
    return future_tasks

async def gather_file_task(bus):
    tasks = []
    for ballot, content in bus:
        filename = make_bu_file_name(ballot)
        task = asyncio.ensure_future(save_file(filename, content))
        tasks.append(task)
    future_tasks = await asyncio.gather(*tasks)
    return future_tasks

def create_batches(batch_size:int, ballots:list)-> list:
    if batch_size > len(ballots):
        return [ballots]
    batches_list = []
    batches =  int(round(len(ballots) / batch_size,0))
    reminder =  len(ballots) % batch_size
    end = 0
    for i in range(batches):
        start = end
        end += batch_size if i < batches else (reminder+1)
        batches_list.append(ballots[start:end])
        start = end
    return batches_list

async def batch_orchestrator(batch):
    async with aiohttp.ClientSession() as session:
        hashes = await gather_hash_tasks(batch, session)
    async with aiohttp.ClientSession() as session:
        bb = await gather_bb_tasks(hashes, session)
    content = await gather_file_task(bb)
    print(content)

if __name__ == '__main__':
    start = perf_counter()
    ballots = get_state_ballot_codes('RR')
    batch_list = create_batches(100, ballots)
    ballots_downloaded = 0
    for batch in batch_list:
        asyncio.run(batch_orchestrator(batch))
        ballots_downloaded += len(batch)
    end = perf_counter()
    print(f"Time elapsed: {end-start}")
    print(f"Total ballots downloaded: {ballots_downloaded}")
    print(f"Average time per ballot: {(end-start)/ballots_downloaded}")
            