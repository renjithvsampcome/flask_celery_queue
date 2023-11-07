
from playwright.sync_api import Page, expect, sync_playwright
import argparse
from playwright_stealth import stealth_sync
from onlyfans import onlyfans_downloader_script
from dotenv import load_dotenv

load_dotenv()

x_bc = None
user_id = None
user_agent = None

def request_handler(request):
    headers = request.headers
    for key, value in headers.items():
        if key == "x-bc":
            global x_bc
            x_bc = value
            # break
        if key == "user-id":
            global user_id
            user_id = value
        if key == "user-agent":
            global user_agent
            user_agent = value
        if all(x is not None for x in (user_agent,user_id,x_bc)):
            break

def test_login(args):
    email = args.email
    pwd = args.password
    vude_id = args.userid

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, slow_mo=100)
            context = browser.new_context()
            page = context.new_page()
            stealth_sync(page)
            page.goto('https://onlyfans.com/')
            
            page.fill('input[name="email"]', email)
            page.fill('input[name="password"]', pwd)
            
            page.click('button[type=submit]')
            page.on("request", request_handler)
            page.locator('a[data-name="Profile"].m-size-lg-hover').click()
            page.on("request", request_handler)
            data = context.cookies("https://onlyfans.com")
            print(data)
            sess = None
            for d in data:
                if d['name'] == "sess":
                    sess = d['value']
            # time.sleep(20)
            clinet_side_values = {
                "x-bc": x_bc,
                "user-id" : user_id,
                "sess" : sess,
                "user-agent" : user_agent,
                "vude-id": vude_id
            }
            try:
                onlyfans_downloader_script(clinet_side_values)
            except Exception as e:
                print(f"error while onlyfans loader: {e}")
    except Exception as e:
        print(f"error while logging onlyfans: {e}")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Playwright Script')
    parser.add_argument('--email', type=str, help='email argument')
    parser.add_argument('--password', type=str, help='pssword argument')
    parser.add_argument('--userid', type=str, help='onlyfans-userid argument')

    args = parser.parse_args()
    test_login(args)

    