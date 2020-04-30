import re
import random
import os
from time import time
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
from queue import SimpleQueue, Empty
from threading import RLock
from concurrent.futures import ThreadPoolExecutor, TimeoutError, as_completed, wait
from urllib.parse import urljoin, urlparse
from extractor import EntityExtractor
from selenium import webdriver
import selenium.common.exceptions as s_exceptions


class MultiThreadScraper:
    def __init__(self, domains_file, offset_file, batch_size, output_file, config='DESKTOP', execute_js=True, ent_extr=False):
        """
        Scrapes info from web-pages parsed from domains_file and puts results into output_file
        :param domains_file: file, with list of domains needed to be parsed
        :param offset_file: line number from domains_file from which should scrape pages
        :param batch_size: amount of domains processing simultaneously
        :param output_file: file where results will be saved
        :param config: on which machine application deployed ('SERVER' or 'DESKTOP')
        :param execute_js: use browser to execute JavaScript
        :param ent_extr: extract entities with Yargy package
        """
        self.config = config
        self.execute_js = execute_js
        self.ent_extr = ent_extr

        self.headers_file = './settings/headers.txt'
        self.proxies_file = './settings/proxies.txt'
        self.domains_file =  os.path.join('./settings/domains/', domains_file)
        self.offset_file = os.path.join('./settings/domains/', offset_file)

        log_name = datetime.strftime(datetime.now(), "%d%m%Y_%H:%M.log") # генерируем имя лог-файла
        logfile_path = os.path.join('./logs/', log_name)
        self.logfile = open(logfile_path, 'w')

        self.log_lock = RLock()  # создаем блокировку для одновременного доступа к файлу с логами
        self.__read_offset() # считываем текущий сдвиг по файлу с доменами
        self.batch_size = batch_size
        self.res_file = os.path.join('./res/', output_file)

        self.prefixes = ['http://', 'https://', ] # список возможный префиксов перед доменным именем
        self.rus_lang_pattern = re.compile(r'[А-Яа-яЁё]')
        self.cookie_pattern = re.compile(r'document\.cookie\s*=')
        self.concat_symbols_pattern = re.compile(r'[\u2010\u2011\u2012\u2013\u2014\u2015]')
        self.contacts_pattern = re.compile(r'\b((о(\s)+нас\b)|(о(\s)+компании\b)|(реквиз)|(контакты)|(обратная связь)|(about)|(rekviz)|(requis)|(contact))') # рег. выражение для проверки, что ссылка ведет на страницу с информацией о компании
        self.inn_pattern = re.compile(r'\bИНН\D{,10}(\d{10,13})\b') # рег. выражение для нахождения ИНН на страницы
        self.ogrn_pattern = re.compile(r'\bОГРН(?:ИП)?\D{,10}(\d{13,15})\b')
        self.phone_pattern = re.compile(r'(?:^|\s)((?:(?:8|\+7)[\- ]?)?\(?\d{3,5}[\)\- ]{1,2}(?:(?:\d{5,7})|(?:\d{1,3}[\- ]?\d{1,2}[\- ]?\d{2,3})))\b')
            # рег. выражение для поиска телефонного номера
        self.email_pattern = re.compile(r'\b([A-Za-z0-9_.+-]+@[A-Za-z0-9-]+\.[A-Za-z0-9-.]+)\b')

        self.driver_lock = RLock()
        if self.execute_js:
            self.__init_webdriver()

        self.extractor = EntityExtractor()

        self.__parse_headers() # собираем хэдеры из файла
        self.__parse_proxies() # собираем ip-адресы прокси-серверов
        self.header = {'User-Agent': self.user_agents[12]} # задаем заголовок

        self.fpages_lock = RLock() # создаем блокировку для доступа к списку найденных страниц
        self.founded_pages = set() # список страниц, которые были обработаны в рамках batch
        self.parsed_pages = set() # список страниц, которые были распарсены в рамках batch

        self.crawl_queue = SimpleQueue() # очередь ссылок для прогрузки (состоит из taple - ссылки для прогрузки и доменного имени из domains_file)
        self.link_tasks = dict() # словарь с заданиями по прогрузке страниц (значение - домен из domains_file)
        self.parse_tasks = list() # список с заданиями по парсингу страниц

    def __del__(self):
        if self.execute_js:
            if self.driver is not None:
                self.driver.quit()

        with self.log_lock:
            self.logfile.flush()  # флашим лог-файл
            self.logfile.close() # закрываем файл с логами

    def __get_cur_date(self):
        return datetime.strftime(datetime.now(), "%d/%m/%Y %H:%M")

    def __read_offset(self): # читаем сдвиг для файла с доменами
        file = open(self.offset_file, 'r')
        self.offset = int(file.readline())
        file.close()

    def __save_offset(self): # записываем сдвиг для файла с доменами
        file = open(self.offset_file, 'w')
        file.write("{:d}".format(self.offset))
        file.close()

    def __init_webdriver(self):
        config = webdriver.DesiredCapabilities.FIREFOX.copy()
        config['platform'] = 'LINUX'
        options = webdriver.firefox.options.Options()
        options.set_preference('webgl.disabled', True)
        options.headless = True
        self.driver = webdriver.Remote("http://localhost:4444/wd/hub", desired_capabilities=config, options=options)
        self.driver.set_page_load_timeout(5)

    def __preproc_cookie(self, cookie_dict):
        avail_names = ['version', 'name', 'value', 'port', 'domain', 'path', 'secure', 'expires', 'discard',
                    'comment', 'comment_url', 'rfc2109']

        res_cookies = {key : value for key, value in cookie_dict.items() if key in avail_names}
        res_cookies['rest'] = {'HttpOnly': cookie_dict.get('httpOnly', None)}
        return res_cookies

    def __parse_headers(self): # чтение хэдеров для подстановки в запросы
        file = open(self.headers_file, 'r')
        self.user_agents = list()
        for line in file:
            self.user_agents.append(line.replace('\n', ''))
        file.close()

    def __parse_proxies(self): # чтение ip-адресов прокси-серверов
        file = open(self.proxies_file, 'r')
        self.proxies = list()
        for line in file:
            self.proxies.append(line.replace('\n', ''))
        file.close()

    def __parse_domains(self): # чтение доменных имен для обработки в рамках batch
        file = open(self.domains_file, 'r')
        for _ in range(self.offset): # переходим на нужную строчку, с которой надо начинать чтение
            next(file)

        self.addrs = list()
        line_cnt = -1
        for line_cnt, line in enumerate(file): # читаем строчки с доменами
            if line_cnt == self.batch_size: # если уже считали необходимое кол-во строчек
                line_cnt -= 1
                break
            else:
                self.addrs.append(line.split('\t')[0].lower().strip()) # добавляем адрес к списку на обработку
        file.close()
        return line_cnt + 1 # возвращаем кол-во считанных строчек

    def __write_results(self, res): # запись результатов обработки batch
        file = open(self.res_file, 'a', encoding="utf-8") # важно помнить про кодировку
        for contact in res:
            file.write("\"{0}\";\"{1}\";\"{2}\";\"{3}\";\"{4}\";\"{5}\";\"{6}\";\"{7}\"\n".format(contact[0], contact[1],\
                        contact[2], contact[3], contact[4], contact[5], contact[6], contact[7])) # записываем контактные данные компании
        file.close()

    def __set_rand_header(self): # установка случайного заголовка для запроса (для эмуляции запросов с браузера)
        self.header = {'User-Agent': random.choice(self.user_agents)}

    def __set_rand_proxy(self): # установка случайного прокси-сервера
        self.proxy = {'http': random.choice(self.proxies)}

    def __fill_queue(self, prefix): # заполнение очереди для прогрузки ссылок
        for addr in self.addrs:
            url = self.prefixes[prefix] + addr # к доменному имени добавляем префикс (либо "http", либо "https")
            depth = 0
            with self.fpages_lock:
                if url not in self.founded_pages:
                    self.crawl_queue.put((url, addr, depth))
                    self.founded_pages.add(url)

    def parse_links(self, response, addr, depth): # парсинг ссылок на контактные данные с загруженной страницы
        base_domain = urlparse(response.url).netloc  # получаем домен корневой страницы
        try:
            soup = BeautifulSoup(response.content, "html.parser") # парсим страницу
        except Exception as e:
            with self.log_lock:
                self.logfile.write("Exception {} on {} at {}\n".format(e, response.url, self.__get_cur_date()))
        else:
            links = soup.find_all('a', href=True) # ищем все теги с ссылками
            for link in links:
                url = link['href'] # извлекаем саму ссылку
                try:
                    url = urljoin(response.url, url, False)
                except Exception as e:
                    continue
                if urlparse(url).netloc == base_domain: # проверяем, что ссылка не ведет на другой веб-сайт
                    link_descr = self.preproc_html(link)
                    if depth > 0 or self.contacts_pattern.search(link_descr.lower()):  # проверяем, есть ли в описании ссылки ключевые слова, относящиеся к контактам
                        with self.fpages_lock:
                            if url not in self.founded_pages: # если данную страницу еще не обрабатывали, то добавляем в очередь
                                self.crawl_queue.put((url, addr, depth + 1))
                                # print("found url: {} on {}".format(url, response.url))

                    with self.fpages_lock: # добавляем найденную ссылку в список найденных
                        self.founded_pages.add(url)

    def scrape_info(self, response, addr, depth): # сбор контактные данных со страницы
        start_time = time()
        try:
            soup = BeautifulSoup(response.content, "html.parser") # парсим страницу
        except Exception as e:
            with self.log_lock:
                self.logfile.write("Exception {} on {} at {}\n".format(e, response.url, self.__get_cur_date()))
            return None
        else:
            if soup.html is not None:
                main_lang = soup.html.get('lang', '').split('-')[0]
                text = self.preproc_html(soup)
                if self.rus_lang_pattern.search(text) or main_lang.lower() == 'ru':
                    inns = self.find_inns(text)
                    ogrns = self.find_ogrns(text)
                    phones = self.find_phones(text)
                    emails = self.find_emails(text)

                    if len(phones) > 0 and self.ent_extr:
                        info = self.extractor.entities_extraction(text)
                        company_names = info.companies
                        cities = info.cities
                    else:
                        company_names = set()
                        cities = set()
                        # if len(company_names) > 1 and depth == 0:
                        #     company_names.clear()
                    if (len(inns) > 0 or len(ogrns) > 0 or (len(company_names) > 0 and depth > 0)) \
                            and len(phones) > 0:
                        inns_str = ', '.join(inns)
                        inns_str = 'ИНН: ' + inns_str
                        ogrns_str = ', '.join(ogrns)
                        ogrns_str = 'ОГРН: ' + ogrns_str
                        company_names_str = ' || '.join(company_names)
                        cities_str = ', '.join((cities))
                        cities_str = 'Город: ' + cities_str
                        phones_str = ', '.join(phones)
                        phones_str = 'Тел.: ' + re.sub('\s+', ' ', phones_str)
                        emails_str = ', '.join(emails)
                        emails_str = 'Почта: ' + emails_str

                        base_url_str = '{}://{}'.format(urlparse(response.url).scheme, urlparse(response.url).netloc)  # получаем адрес корневой страницы
                        res = (base_url_str, response.url, company_names_str, inns_str, ogrns_str, cities_str,\
                               phones_str, emails_str) # записываем результат
                    else:
                        res = None
                    # with self.log_lock:
                    #     print("found {}".format(res))
                    #     self.logfile.write("parse url {} takes {}s\n".format(response.url, time() - start_time))
                else:
                    res = None
            else:
                res = None
            return res

    def make_request(self, session, url):
        try:
            resp = session.get(url, timeout=(3, 2.5))  # делаем запрос
        except requests.RequestException as e:  # если ошибки при запросе
            # with self.log_lock:
            #     self.logfile.write("Exception {} on {}\n".format(e, url))
            return None
        except Exception as e:  # если остальные ошибки
            with self.log_lock:
                self.logfile.write("Exception {} on {} at {}\n".format(e, url, self.__get_cur_date()))
            return None
        else:
            return resp

    def check_page_response(self, resp):
        if resp is not None and resp.status_code == requests.codes.ok:
            headers = resp.headers.get('Content-Type')  # получаем mime-тип страницы
            if headers is not None and headers.split(';')[0] == 'text/html':  # если тип страницы html
                res_resp = resp
            else:  # тип страницы не соответствует html
                # with self.log_lock:
                #     self.logfile.write("No header or content-type on {}\n".format(resp.url))
                res_resp = None
        else:
            res_resp = None

        return res_resp

    def get_page(self, url): # получение страницы
        with requests.Session() as session:
            session.headers = self.header
            if self.config == 'SERVER':
                session.verify = '/etc/pki/tls/certs/ca-bundle.crt'
            resp = self.make_request(session, url)
            if resp is not None:
                with self.fpages_lock:
                    self.founded_pages.add(resp.url)
            resp = self.check_page_response(resp)

            if resp is not None and self.execute_js:
                try:
                    soup = BeautifulSoup(resp.content, "html.parser")  # парсим страницу
                except Exception as e:
                    with self.log_lock:
                        self.logfile.write(
                            "Exception {} on {} at {}\n".format(e, resp.url, self.__get_cur_date()))
                    resp = None
                else:
                    found_cookies = None
                    for script in soup(["script"]):
                        script_text = script.getText(separator='\n', strip=True)
                        found_cookies = self.cookie_pattern.search(script_text)
                        if found_cookies is not None:
                            break

                    if found_cookies is not None:
                        with self.driver_lock:
                            try:
                                self.driver.get(url)
                                cookies = self.driver.get_cookies()
                                self.driver.delete_all_cookies()
                            except s_exceptions.TimeoutException:
                                cookies = list()
                            except s_exceptions.WebDriverException as e:
                                with self.log_lock:
                                    self.logfile.write(
                                        "Exception {} on {} at {}\n".format(e, resp.url, self.__get_cur_date()))
                                self.__init_webdriver()
                                cookies = list()

                            for cookie in cookies:
                                session.cookies.set(**self.__preproc_cookie(cookie))

                    if len(session.cookies) > 0:
                        resp = self.make_request(session, resp.url)
                        resp = self.check_page_response(resp)
            return resp

    def scrape_page(self, resp, addr, depth): # парсинг загруженной страницы
        # with self.log_lock:
        #     print("Start parse on {}\n".format(resp.url))
        # start_time = time()
        res = self.scrape_info(resp, addr, depth) # пытаемся собрать информацию
        if depth < 2 and res is None: # если не удалось собрать информацию и необходимо парсить ссылки
            self.parse_links(resp, addr, depth) # парсим ссылки
        # with self.log_lock:
        #     self.logfile.write("scrape page {} takes {}s".format(resp.url, time() - start_time))

        # with self.log_lock:
        #     print("End parse on {}\n".format(resp.url))
        return res

    def run_scraper(self): # запуск краулера
        start_time = time() # запоминаем время запуска
        with self.log_lock:
            self.logfile.write("Execution start at {}\n".format(self.__get_cur_date())) # Пишем время запуска

        if self.config == 'SERVER':
            load_workers = 256
            parse_workers = 32
        else:
            load_workers = 120
            parse_workers = 12

        with ThreadPoolExecutor(max_workers=load_workers) as load_pool, ThreadPoolExecutor(max_workers=parse_workers) as prs_pool: # создаем пул тредов
            line_cnt = self.__parse_domains()  # получаем домены для обработки
            prefix = 0 # используемый префикс (http)
            self.__fill_queue(prefix) # заполняем очередь, используя префикс http перед доменными именами
            links_cnt = 0 # кол-во ссылок, к которым выполнялись запросы
            valid_links_cnt = 0 # кол-во ссылок, по которым удалось выгрузить страницы
            while line_cnt != 0: # выполняем главный цикл, пока остались необработанные домены
                start_req = time() # время начала запросов по ссылкам из очереди
                while True: # цикл, пока очередь не опустеет
                    try:
                        target_url, addr, depth = self.crawl_queue.get_nowait() # получение ссылок из очереди
                        task = load_pool.submit(self.get_page, target_url) # создаем задачу для тредов
                        self.link_tasks[task] = (target_url, addr, depth)  # записываем идентификатор задачи в словарь задач на прогрузку
                        links_cnt += 1
                    except Empty: # если очередь пуста
                        break
                    except Exception as e:
                        print(e)

                if self.config == 'SERVER':
                    cores = 8
                else:
                    cores = 4
                runtime_timeout = len(self.link_tasks.keys()) / (cores * 0.8) * 6 # считаем предельное время выполнения всех запросов на прогрузку (с учетом кол-ва ядер)
                # with self.log_lock:
                #     self.logfile.write('timeout links {}'.format(runtime_timeout))

                try:
                    for link_task in as_completed(self.link_tasks.keys(), runtime_timeout): # цикл по завершенным задачам прогрузки за предельное время
                        task_res = link_task.result() # получаем результат прогрузки
                        if task_res is not None: # если сервер корректно ответил и ответ не пустой
                            valid_links_cnt += 1
                            # with self.log_lock:
                            #     self.logfile.write('request done: {}'.format(task_res.url))
                            #     print('request done: {}'.format(task_res.url))
                            url, addr, depth = self.link_tasks[link_task] # получаем доменное имя, к которому относится страница
                            if depth == 0: # если получали верхнеур. страницу
                                self.addrs.remove(addr) # то удаляем из списка доменов для обхода

                            if task_res.url not in self.parsed_pages:  # если данную страницу еще не парсили
                                self.parsed_pages.add(task_res.url)  # добавляем к списку распарсенных страниц
                                task = prs_pool.submit(self.scrape_page, task_res, addr, depth)  # создаем задачу на парсинг данной страницы (с указанем текущей глубины страницы)
                                self.parse_tasks.append(task)  # добавляем идентификатор задачи к списку задач парсинга

                        self.link_tasks.pop(link_task) # убираем данную задачу из словаря задача прогрузки
                except TimeoutError as err: # если остались задачи, не выполненные за предельное время
                    for link_task in self.link_tasks.keys(): # отменяем не выполненные задачи
                        link_task.cancel()
                        url, addr, depth = self.link_tasks[link_task]  # получаем доменное имя, к которому относится страница
                        with self.log_lock:
                            self.logfile.write("thread processing timeout on site {}\n".format(url))
                    self.link_tasks.clear() # очищаем словарь с задачами прогрузки

                with self.log_lock: # записываем сколько время заняла выгрузка
                    self.logfile.write("requests takes {:.3f}s\n".format(time() - start_req))
                founded_contacts = list() # список с найденными контактными данными
                for parse_task in as_completed(self.parse_tasks): # цикл по всем завершенным задачам парсинга
                    task_res = parse_task.result() # получаем результат парсинга контактных данных
                    if task_res is not None: # если нашли данные
                        founded_contacts.append(task_res)
                    self.parse_tasks.remove(parse_task) # удаляем задачу из списка задач парсинга
                self.__write_results(set(founded_contacts)) # записываем контакты в файл
                with self.log_lock: # выводим статистику по кол-во обработанных ссылок
                    self.logfile.write("{:.2%} pages were loaded from {} processed links\n".format(valid_links_cnt / links_cnt, links_cnt))

                if self.crawl_queue.empty(): # если очередь ссылок на прогрузку пуста
                    if prefix == 0 and len(self.addrs) > 0: # если префикс http и остались страницы, не давшие ответа
                        prefix = 1 # меняем префикс на https
                    else:
                        with self.fpages_lock:
                            self.founded_pages.clear()  # очищаем список выгружаемых ссылок
                        self.parsed_pages.clear()  # очищаем список страниц для парсинга
                        self.offset += line_cnt  # увеличиваем сдвиг в файле с доменными адресами
                        self.__save_offset()  # сохраняем сдвиг
                        with self.log_lock:
                            self.logfile.write("{}\n".format(self.__get_cur_date())) # Записываем время
                            self.logfile.write("\t{:.2%} domain names are not valids from batch\n".format(len(self.addrs) / line_cnt)) # Записываем долю невалидных доменов
                            self.logfile.write("\toffset: {}\n\n".format(self.offset)) # записываем сдвиг в файл
                            self.logfile.flush()  # флашим лог-файл

                        line_cnt = self.__parse_domains()  # получаем новую порцию адресов для обработки
                        prefix = 0 # ставим префикс http
                        # CHANGE PARAMETERS
                        # if self.offset == 945000:
                        #     line_cnt = 0

                    self.__fill_queue(prefix) # заполняем очередь

            with self.log_lock: # записываем статистику выполнения
                self.logfile.write("Execution finished at {}\n".format(self.__get_cur_date()))
                exec_time = timedelta(seconds=(time() - start_time))
                self.logfile.write("Execution takes {}\n".format(exec_time))

    def preproc_html(self, soup):
        for script in soup(["script", "style"]):
            script.decompose()

        text = soup.getText(separator='\n', strip=True)
        text = self.concat_symbols_pattern.sub('-', text)

        lines = [line for line in text.splitlines()]
        if len(lines) > 10000:
            lines = lines[:5000] + lines[-5000:]
        tmp_lines = list()
        for line in lines:
            line = re.sub(r'\s+', ' ', line)
            if len(tmp_lines) == 0:
                tmp_lines.append(line)
            elif re.search(r'^([A-ZА-ЯЁ]|\d\.)', line) and not re.search(r'[:;, \xad\-«„ʼ"\'”]$', tmp_lines[-1]):
                tmp_lines.append(line)
            else:
                if re.search(r'[\xad«„ʼ]$', tmp_lines[-1]) or re.search(r'^[^\w\d«„ʼ"\'”]', line):
                    tmp_lines[-1] = tmp_lines[-1] + line
                else:
                    tmp_lines[-1] = tmp_lines[-1].strip() + ' ' + line

        lines.clear()
        for line in tmp_lines:
            line = line.replace('\xad', '')
            if not re.search(r'[:;?!.]$', line):
                lines.append(line + '.')
            else:
                lines.append(line)

        text = ' '.join(lines)
        return text

    def check_inn(self, inn): # проверка корректности ИНН по контрольным суммам
        inn = str(inn)
        digits = [int(x) for x in inn]
        if len(digits) == 12: # проверка для ИП и ФЛ
            coeff1 = [7, 2, 4, 10, 3, 5, 9, 4, 6, 8]
            checksum1 = sum([x * digits[i] for i, x in enumerate(coeff1)]) % 11 % 10
            coeff2 = [3, 7, 2, 4, 10, 3, 5, 9, 4, 6, 8]
            checksum2 = sum([x * digits[i] for i, x in enumerate(coeff2)]) % 11 % 10

            if checksum1 == digits[-2] and checksum2 == digits[-1]:
                return True
            else:
                return False
        elif len(digits) == 10: # проверка для ЮЛ
            coeff = [2, 4, 10, 3, 5, 9, 4, 6, 8, 0]
            checksum = sum([x * digits[i] for i, x in enumerate(coeff)]) % 11 % 10
            if checksum == digits[-1]:
                return True
            else:
                return False
        else:
            return False

    def check_ogrn(self, ogrn):
        ogrn = str(ogrn)
        if len(ogrn) == 15:
            checksum = int(ogrn[:-1]) % 13 % 10
            if checksum == int(ogrn[-1]):
                return True
            else:
                return False
        elif len(ogrn) == 13:
            checksum = int(ogrn[:-1]) % 11 % 10
            if checksum == int(ogrn[-1]):
                return True
            else:
                return False
        else:
            return False

    def find_inns(self, text):
        inns = set(self.inn_pattern.findall(text))  # ищем инн на странице
        inns = [x for x in inns if self.check_inn(x)]  # проверяем инн на корректность
        return inns

    def find_ogrns(self, text):
        ogrns = set(self.ogrn_pattern.findall(text))
        ogrns = [x for x in ogrns if self.check_ogrn(x)]
        return ogrns

    def find_phones(self, text):
        phones = self.phone_pattern.findall(text) # ищем номера телефонов на странице

        res = dict()
        for phone in phones:
            num_phone = re.sub(r'\D', '', phone)
            if len(num_phone) == 10 or len(num_phone) == 11:
                if len(num_phone) == 10:
                    num_phone = '8' + num_phone
                    phone = '+7' + phone.strip()
                else:
                    num_phone = '8' + num_phone[1:]

                if num_phone in res:
                    res[num_phone]['cnt'] += 1
                else:
                    res[num_phone] = {'orig' : phone, 'cnt' : 1}

        res = sorted(res.items(), key= lambda x : x[1]['cnt'], reverse= True)
        res = [x[1]['orig'] for x in res]
        return res

    def find_emails(self, text):
        emails = self.email_pattern.findall(text)
        res = dict()
        for email in emails:
            if email in res:
                res[email] += 1
            else:
                res[email] = 1

        res = sorted(res.items(), key=lambda x: x[1], reverse=True)
        res = [x[0] for x in res]
        return res
