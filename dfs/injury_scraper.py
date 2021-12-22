import os
from tqdm import tqdm
from selenium import webdriver
import pandas as pd
from datetime import date
from pyvirtualdisplay import Display

'''
Tutorial of scraping injury information from 
https://medium.com/analytics-vidhya/monitoring-nba-injuries-with-python-bf05ea2aec68

Additional dependencies:
https://stackoverflow.com/questions/40188699/webdriverexception-message-geckodriver-executable-needs-to-be-in-path
https://stackoverflow.com/questions/16180428/can-selenium-webdriver-open-browser-windows-silently-in-the-background
'''


########################### I - First define the functions
def ffi(a):
    try:
        return(driver.find_element_by_xpath(a).text)
    except:
        return(None)

def collect_info_line(team, i):
    TEAM = ffi('//*[@id="injury-report-page-wrapper"]/div/div[2]/div[{}]/a/div/div[2]'.format(team))
    player = ffi('//*[@id="injury-report-page-wrapper"]/div/div[2]/div[{}]/div/div/div[1]/table/tbody/tr[{}]/td[1]/span/a'.format(team, i))
    position = ffi('//*[@id="injury-report-page-wrapper"]/div/div[2]/div[{}]/div/div/div[1]/table/tbody/tr[{}]/td[2]'.format(team, i))
    status = ffi('//*[@id="injury-report-page-wrapper"]/div/div[2]/div[{}]/div/div/div[1]/table/tbody/tr[{}]/td[3]'.format(team, i))
    date = ffi('//*[@id="injury-report-page-wrapper"]/div/div[2]/div[{}]/div/div/div[1]/table/tbody/tr[{}]/td[4]'.format(team, i))
    injury = ffi('//*[@id="injury-report-page-wrapper"]/div/div[2]/div[{}]/div/div/div[1]/table/tbody/tr[{}]/td[5]'.format(team, i))
    returns = ffi('//*[@id="injury-report-page-wrapper"]/div/div[2]/div[{}]/div/div/div[1]/table/tbody/tr[{}]/td[6]'.format(team, i))

    # if player is not None:
    #     print(TEAM, player, status)
    return([TEAM, player, position, status, date, injury, returns])

def scrape():
    '''
    Scrapes in the injury report from rotoworld.
    Args:
        None
    Returns:
        Dataframe of all the current injuries
    '''

    print("[Injury Scraper] Scraping all injurties from RotoWorld. Connecting to webdriver in background...", end=" ")
    display = Display(visible=0, size=(800, 600))
    display.start()
    global driver


    driver = webdriver.Firefox()
    driver.get('https://www.rotoworld.com/basketball/nba/injury-report')
    # driver.get('https://www.espn.com/nba/injuries')
    print("SUCCESS")

    ########################## II - Now scrape the page...
    Injuries = []
    for team in tqdm(range(1, 31), desc='Compiling injury report for all teams'):
        for i in range(1, 50):
            info = collect_info_line(team, i)
            if info[1] is not None:
                Injuries.append(info)
            else: # if we have info[1] is None it means that there is no more injuries for this team : we go to next team
                break

    ########################## III - Save the csv and send by mail
    Injuries = pd.DataFrame(Injuries)
    Injuries.columns = ['Team', 'Player', 'Position', 'Status', 'News date', 'Injury', 'Return date']
    Injuries.to_csv('Injuries_report_{}.csv'.format(date.today()), index = False, sep = ';')

    # update names:
    for i, row in Injuries.iterrows():
        if row.Player in ROTOWORLD_PLAYER_NAME_TO_DK_NAME:
            Injuries.at[row.name, "Player"] = ROTOWORLD_PLAYER_NAME_TO_DK_NAME[row.Player]

    driver.quit()
    display.stop()

    return Injuries

def convert_name(rotoworld_name):
    if rotoworld_name in ROTOWORLD_PLAYER_NAME_TO_DK_NAME:
        return ROTOWORLD_PLAYER_NAME_TO_DK_NAME[rotoworld_name]
    else:
        return rotoworld_name


ROTOWORLD_PLAYER_NAME_TO_DK_NAME = {
    "Marcus Morris":"Marcus Morris Sr."
}