from contactsScraper import MultiThreadScraper
from settings.config import MACHINE, USE_BROWSER, EXTRACT_ALL_ENTITIES
import sys, resource



def main():
    gb_bytes = 1024 * 1024 * 1024
    if MACHINE == 'SERVER':
        resource.setrlimit(resource.RLIMIT_DATA, (26 * gb_bytes, 29 * gb_bytes))
    else:
        resource.setrlimit(resource.RLIMIT_DATA, (3.6 * gb_bytes, 4.2 * gb_bytes))
    domains_file = sys.argv[1]
    offset_file = sys.argv[2]
    batch_size = int(sys.argv[3])
    result_file = sys.argv[4]
    crawler = MultiThreadScraper(domains_file, offset_file, batch_size, result_file, config=MACHINE, execute_js=USE_BROWSER, ent_extr=EXTRACT_ALL_ENTITIES)
    crawler.run_scraper()

if __name__ == '__main__':
    main()