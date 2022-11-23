import logging
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

class Myconfig():
    def __init__(self, config):
        self.config = config


    def crawl_link_league(config_sport, queue_league):
        for step in config_sport['step_get_link_league']:
            if step == 'send_request':
                browser = send_request(config_sport['website'])
            else:
                detect_step(step, browser, config_sport, queue_league, "")
        browser.close()


    def crawl_detail(config_sport, link_league):
        for step in config_sport['step_get_detail']:
            if step == 'send_request':
                browser = send_request(config_sport['website'])
            else:
                detect_step(step, browser, config_sport, link_league)
        browser.close()
            

    def send_request(url):
        # global browser
        options = Options()
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        options.headless = False
        browser = webdriver.Chrome(executable_path='D:\chrome_driver\chromedriver_win32\chromedriver.exe', options=options)
        browser.implicitly_wait(10)
        browser.get(url)
        time.sleep(2)
        return browser


    def detect_step(step, browser, config_sport, queue_league, link_league):
        if step == "accept_cookies":
            detect_type_action(browser, config_sport['accept_cookies'])
        elif step == "find_my_league":
            find_my_league(config_sport['id'], browser, config_sport['find_my_league'], queue_league)
        elif step == "show_more_country":
            show_more_country(browser, config_sport['show_more_country'])
        elif step == "show_league_VN":
            detect_type_action(browser, config_sport['show_league_VN'])
        elif step == "find_VN_league":
            find_VN_league(config_sport['id'], browser, config_sport['find_VN_league'], queue_league)
        elif step == "show_results":
            show_results(browser, config_sport['show_results'])
        elif step == "show_more_content":
            show_more_content(browser, config_sport['show_more_content'])
        elif step == "craw_detail":
            craw_detail(browser, config_sport['craw_detail'])
        elif step == "show_fixtures":
            show_fixtures()
        elif step == "craw_detail":
            craw_detail()



    def detect_type_action(browser, config_action):
        if config_action['type_action'] == 1:
            action_click(browser, config_action)
        elif config_action['type_action'] == 2:
            list_link = action_get_attribute(browser, config_action)
            return list_link


    def action_click(browser, config):
        browser.find_element(By.XPATH, config['xpath']).click()


    def action_get_attribute(browser, config):
        list_link = []
        elements = browser.find_elements(By.XPATH, config['xpath'])
        logging.info(config['xpath'])
        for element in elements:
            link = element.get_attribute(config['attribute'])
            if link:
                list_link.append(link)
        return list_link


    def find_my_league(id, browser, config_find_my_league, queue_league):
        time.sleep(int(config_find_my_league['time_sleep']))
        list_link_league = detect_type_action(browser, config_find_my_league)
        for link_league in list_link_league:
            queue_league.put({"id_config":id, "link_league":link_league})


    def show_more_country(browser, config_show_more_country):
        time.sleep(int(config_show_more_country['time_sleep']))
        detect_type_action(browser, config_show_more_country)


    def find_VN_league(id, browser, config_find_VN_league, queue_league):
        time.sleep(int(config_find_VN_league['time_sleep']))
        list_link_league = detect_type_action(browser, config_find_VN_league)
        for link_league in list_link_league:
            queue_league.put({"id_config":id, "link_league":link_league})


    def show_results(browser, config_show_results):
        detect_type_action(browser, config_show_results)    
        time.sleep(int(config_show_results['time_sleep']))


    def show_more_content(browser, config_show_more_content):
        try:
            while browser.find_element(By.XPATH, config_show_more_content['xpath']):
                detect_type_action(browser, config_show_more_content)
        except:
            pass


    def craw_detail():
        pass


    def show_fixtures():
        pass


    def crawl_detail():
        pass






