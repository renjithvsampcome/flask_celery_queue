
from playwright.sync_api import Page, expect, sync_playwright
from playwright_recaptcha import recaptchav3, recaptchav2
from playwright_stealth import stealth_sync
from onlyfans import onlyfans_downloader_script
from dotenv import load_dotenv
import os
import time

load_dotenv()



def test_login(email, pwd, vude_id):
    # email = args.email
    # pwd = args.password
    # vude_id = args.userid

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, slow_mo=100)
            context = browser.new_context()
            page = context.new_page()
            stealth_sync(page)
            page.goto('https://onlyfans.com/')
            with recaptchav2.SyncSolver(page,capsolver_api_key=os.environ.get('CAPSOLVER_KEY')) as solver:
                
                page.fill('input[name="email"]', email)
                time.sleep(1)
                page.fill('input[name="password"]', pwd)
                page.click('button[type=submit]')
                token = solver.solve_recaptcha(wait=True,image_challenge=True)
                print(token)
                page.click('button[type=submit]')

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
                    raise Exception(f"onlyfans scrapper got crashed")
                
            else:
                raise Exception(f"error while logging onlyfans, client_side_value_missing")
                
    except Exception as e:
        print(f"error while logging onlyfans: {e}")
        # raise Exception(f"error while onlyfans loader: {e}")
        return False


    