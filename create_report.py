import json


STATE_BALLOTS_FILE_PATH = "./state_ballots/{uf}-p000407-cs.json"

def calculate_state_ballot_report(state_code:str)-> list:
    total_ballots = 0
    with open(STATE_BALLOTS_FILE_PATH.format(uf=state_code.lower()), "r+") as fp:
        state_data = json.load(fp)

    state_code = state_data["abr"][0]["cd"]
    councils  = state_data["abr"][0]["mu"]

    for council in councils:
        zones = council["zon"]
        for zone in zones:
            sections = zone["sec"]
            for _ in sections:
                total_ballots += 1

    return {'state': f'{state_code}', 'no_concils': f'{len(councils)}', 'total_ballots': f'{total_ballots}'}

def count_total_ballots(state_reports:list)->int:
    total = 0
    for state in state_reports:
        total += int(state['total_ballots'])
    return total

if __name__ == '__main__':
    with open("brazil-states.json", "r+") as fp:
        states = json.load(fp)
        states_reports = []
        for state in states.keys():
            item = calculate_state_ballot_report(state)
            states_reports.append(item)
    total_ballots = count_total_ballots(states_reports)
    print(f'total ballot boxes to download {total_ballots}')
    print(f'time take to download all syncrounously {total_ballots * 2.38/60/60/24:.0f} days')
    print(f'time take to download all Asyncrounously {total_ballots * 0.099/60/60:.0f} hours')
    with open("states_reports.json", "w+") as fp:
        json.dump(states_reports, fp, indent=4) 

