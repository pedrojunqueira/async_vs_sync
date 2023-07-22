import json
from time import perf_counter

import requests

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

def get_hash(ballot_codes:dict)-> str:
    
    url_request = URL_HASH.format(uf=ballot_codes["state_code"], mu_cd=ballot_codes["mu_cd"], 
                                    zone_cd=ballot_codes["zone_cd"],section_cd=ballot_codes["section_cd"])

    headers = {'Accept': 'application/json, text/plain, */*'}

    response = requests.get(url_request, headers=headers)

    hash_data  = response.json()
    hash_code = hash_data["hashes"][0]["hash"]
    
    return hash_code

def make_bu_file_name(ballot_codes:dict)-> str:
    mu_cd = ballot_codes['mu_cd']
    zone_cd = ballot_codes['zone_cd']
    section_cd = ballot_codes['section_cd']
    return f"o00407-{mu_cd}{zone_cd}{section_cd}.bu"


def get_ballot_bu(ballot_codes:dict, hash_code:str)->None:

    url_request = URL_BU.format(uf=ballot_codes["state_code"], mu_cd=ballot_codes["mu_cd"], 
                                    zone_cd=ballot_codes["zone_cd"],section_cd=ballot_codes["section_cd"],hash=hash_code)

    response = requests.get(url_request)

    file_name = make_bu_file_name(ballot_codes)

    with open(f"./ballot_files/{file_name}", "wb") as fp:
        fp.write(response.content)

    print(f"file: {file_name} saved successfully")

if __name__ == '__main__':

    start = perf_counter()
    ballots = get_state_ballot_codes('RR')
    ballots_downloaded = 0
    for ballot in ballots[:2]:
        hash = get_hash(ballot)
        get_ballot_bu(ballot, hash)
        ballots_downloaded += 1
    end = perf_counter()
    print(f"Time elapsed: {end-start}")
    print(f"Total ballots downloaded: {ballots_downloaded}")
    print(f"Average time per ballot: {(end-start)/ballots_downloaded}")
            