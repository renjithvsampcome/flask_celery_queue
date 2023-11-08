
from playwright.sync_api import Page, expect, sync_playwright
from playwright_recaptcha import recaptchav3
from onlyfans import onlyfans_downloader_script
from dotenv import load_dotenv

load_dotenv()

x_bc = None
user_id = None
user_agent = None


def test_login(email, pwd, vude_id):
    # email = args.email
    # pwd = args.password
    # vude_id = args.userid

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, slow_mo=100)
            context = browser.new_context()
            page = context.new_page()
            with recaptchav3.SyncSolver(page) as solver:
                page.goto("https://antcpt.com/score_detector/")
                token = solver.solve_recaptcha()
            page.goto('https://onlyfans.com/')
            
            page.fill('input[name="email"]', email)
            page.fill('input[name="password"]', pwd)
            page.click('button[type=submit]')
            # page.locator('a[data-name="Profile"].m-size-lg-hover').click()
            
            data = context.cookies("https://onlyfans.com")
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
            clinet_side_values = {
                "x-bc": x_bc,
                "user-id" : user_id,
                "sess" : sess,
                "user-agent" : "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
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

    