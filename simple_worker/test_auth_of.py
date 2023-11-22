
from playwright.sync_api import Page, expect, sync_playwright
from playwright_recaptcha import recaptchav3, recaptchav2
from playwright_stealth import stealth_sync
from onlyfans import onlyfans_downloader_script
from dotenv import load_dotenv
import os
import time

load_dotenv()

x_bc = None
user_id = None
user_agent = None

def request_handler(request):
    headers = request.headers
    url = request.url
    method = request.method
    print(f"Request URL: {url}")
    print(f"Request Method: {method}")
    print("Request Headers:")
    for key, value in headers.items():
        print(f"{key}: {value}")
    print("-----------------------------")



def test_login(email, pwd, vude_id):
    # email = args.email
    # pwd = args.password
    # vude_id = args.userid

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, slow_mo=100)
            # ua = (
            # "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            # "AppleWebKit/537.36 (KHTML, like Gecko) "
            # "Chrome/69.0.3497.100 Safari/537.36"
            # )
            context = browser.new_context()
            context = browser.new_context()
            page = context.new_page()
            stealth_sync(page)
            page.goto('https://onlyfans.com/')
            with recaptchav2.SyncSolver(page,capsolver_api_key="CAP-7ACC999B67FAEA2BBD1020DAD1DA00B7") as solver:
                
                page.fill('input[name="email"]', email)
                time.sleep(1)
                page.fill('input[name="password"]', pwd)
                page.click('button[type=submit]')
                # page.goto("https://antcpt.com/score_detector/")
                token = solver.solve_recaptcha(wait=True,image_challenge=True)
                print(token)
                page.click('button[type=submit]')
                page.on("request", request_handler)
            # page.on("request", request_handler)

            time.sleep(15)
            data = context.cookies("https://onlyfans.com")  
            # print(data)
            sess = None
            x_bc = None
            user_id = None
            for d in data:
                if d['name'] == "sess":
                    sess = d['value']
                if d['name'] == "fp":
                    x_bc = d['value']
                if d['name'] == "auth_id":
                    user_id = d['value']
            # time.sleep(20)
            # print(data)
            clinet_side_values = {
                "x-bc": x_bc,
                "user-id" : user_id,
                "sess" : sess,
                "user-agent" : 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) HeadlessChrome/120.0.6099.28 Safari/537.36',
                "vude-id": vude_id
            }
            print(clinet_side_values)
            if all(value is not None for value in clinet_side_values.values()):
                try:
                    onlyfans_downloader_script(clinet_side_values)
                except Exception as e:
                    print(f"Error from onlyfans downloader: {e}")
                
            else:
                print(f"auth not complete encounter some error")
                raise Exception(f"error while logging onlyfans, client_side_value_missing")
                
    except Exception as e:
        print(f"error while logging onlyfans: {e}")
        # raise Exception(f"error while onlyfans loader: {e}")
        return False


# if __name__ == '__main__':
#     parser = argparse.ArgumentParser(description='Playwright Script')
#     parser.add_argument('--email', type=str, help='email argument')
#     parser.add_argument('--password', type=str, help='pssword argument')
#     parser.add_argument('--userid', type=str, help='onlyfans-userid argument')

#     args = parser.parse_args()
#     test_login(args)

    