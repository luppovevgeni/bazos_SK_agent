import requests
from time import perf_counter, sleep
from datetime import datetime, timedelta
from bson import ObjectId
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError, Locator, Error as PWError
from pymongo import ReturnDocument
import db
import os
from bs4 import BeautifulSoup
import math, random, time, pyautogui, re
from typing import Tuple, Optional
from requests import Session
from zoneinfo import ZoneInfo
from multiprocessing import Process, Queue
from queue import Empty as QueueEmpty


os.system("")

PAGE_LOAD_TIMEOUT_MS = 60000
SPAM_TIMEOUT_S = 400
WAIT_S = (400, 800)
WAIT_M = (1000, 1500)
pyautogui.FAILSAFE = True
pyautogui.PAUSE = 0
MSK = ZoneInfo("Europe/Moscow")

TOKEN = "eyJhbGciOiJIUzUxMiJ9.eyJicGRzLmJ1Y2tldCI6Im1seC1icGRzLXByb2QtZXUtMSIsIm1hY2hpbmVJRCI6IiIsInByb2R1Y3RJRCI6IiIsIndvcmtzcGFjZVJvbGUiOiJvd25lciIsInZlcmlmaWVkIjp0cnVlLCJzaGFyZElEIjoiY2JlMTM4MDAtYmJhZi00YzhmLTgwYjMtMTk3Zjg5NjM5NGYyIiwidXNlcklEIjoiOGNlN2NiNGQtZjgxYS00ODRjLWFkZjMtMmNiZDE1NjI1N2E0IiwiZW1haWwiOiJhbmRyZXltYWlsMDVAbWFpbC5ydSIsImlzQXV0b21hdGlvbiI6dHJ1ZSwid29ya3NwYWNlSUQiOiI0OGQ4ZDlhOS01ZTBjLTQ2MzgtYjAxNC01MjMyMGNkNjdkYTMiLCJqdGkiOiI3NDFiODFlZC1lNmJmLTRkMjAtODkwNS02YTQyMzQ1NjYwZWQiLCJzdWIiOiJNTFgiLCJpc3MiOiI4Y2U3Y2I0ZC1mODFhLTQ4NGMtYWRmMy0yY2JkMTU2MjU3YTQiLCJpYXQiOjE3NTc3NzQ4MjQsImV4cCI6MTc2MDM2NjgyNH0.R1AqG50kkOnR3KqREFIxRfoJqUWzMiMLENsHnHDv96c0w3V5rSfrxC-VioezFSZGYyqbALvlWZ1j01QS1yeJEA"
HEADERS = {
    'Accept': 'application/json',
    'Content-Type': 'application/json',
    'Authorization': f'Bearer {TOKEN}'
}

def get_page_number(url, page, name):
    page -= 1
    session = Session()
    session.headers.update({'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36'})
    for i in range(28):
        full_url = f"{url}/{(page + i) * 20}/"
        response = session.get(full_url)
        soup = BeautifulSoup(response.text, 'html.parser')
        items = soup.find_all('div', class_='inzeraty inzeratyflex')
        for item in items:
            item_name = item.find('h2', class_='nadpis').get_text(strip=True)
            if name == item_name:
                return page + i + 1
    return False

def color_text(text, color_code='92'):
    return f"\033[{color_code}m{text}\033[0m"

def log(text, level="info"):
    db.logs.insert_one({
        "text": text,
        "level": level,
        "agent": SERVER_NAME})
    print(f"{color_text(datetime.now().strftime('%H:%M:%S'))}: {text}")

def _sleep_ms(a: int, b: Optional[int] = None):
    if b is None:
        time.sleep(a / 1000.0)
    else:
        time.sleep(random.uniform(a, b) / 1000.0)

def wait_stable(page, *, dom_timeout=PAGE_LOAD_TIMEOUT_MS, idle_timeout=PAGE_LOAD_TIMEOUT_MS, quiet_ms=WAIT_S):
    try:
        page.wait_for_load_state("domcontentloaded", timeout=dom_timeout)
    except Exception:
        pass
    try:
        page.wait_for_load_state("networkidle", timeout=idle_timeout)
    except Exception:
        pass
    _sleep_ms(*quiet_ms)

def safe_evaluate(page, script, arg=None, retries=3):
    for _ in range(retries):
        try:
            return page.evaluate(script) if arg is None else page.evaluate(script, arg)
        except PWError as e:
            msg = str(e)
            if "Execution context was destroyed" in msg or "page is navigating" in msg:
                wait_stable(page)
                continue
            raise
    return page.evaluate(script) if arg is None else page.evaluate(script, arg)

def _get_window_metrics(page):
    return page.evaluate("""
(() => {
  const sx = window.screenX || window.screenLeft || 0;
  const sy = window.screenY || window.screenTop || 0;
  const innerW = window.innerWidth, innerH = window.innerHeight;
  const outerW = window.outerWidth, outerH = window.outerHeight;
  const dpr = window.devicePixelRatio || 1;
  const borderX = Math.max(0, (outerW - innerW) / 2);
  const borderTop = Math.max(0, outerH - innerH - borderX);
  const vv = window.visualViewport || { offsetLeft: 0, offsetTop: 0, pageLeft: 0, pageTop: 0 };
  return { sx, sy, dpr, borderX, borderTop, vx: vv.offsetLeft||0, vy: vv.offsetTop||0, px: vv.pageLeft||0, py: vv.pageTop||0 };
})()
""")

def _box_to_screen_xy(page, box, bias_center: float = 0.6) -> Tuple[float,float,float]:
    m = _get_window_metrics(page)
    cx = box["x"] + box["width"]  * (0.5 + random.uniform(-0.5*(1-bias_center), 0.5*(1-bias_center)))
    cy = box["y"] + box["height"] * (0.5 + random.uniform(-0.5*(1-bias_center), 0.5*(1-bias_center)))
    x = m["sx"] + m["borderX"]  + (cx - m["vx"])
    y = m["sy"] + m["borderTop"] + (cy - m["vy"])
    W = max(6.0, min(box["width"], box["height"]))
    return (x, y, W)

def _minimum_jerk_profile(t: float) -> float:
    return 10*t**3 - 15*t**4 + 6*t**5

def _ease_samples(duration: float, base_hz: float = 240.0):
    n = max(12, int(duration * base_hz))
    ts = []
    for i in range(n):
        t = (i+1)/n
        w = _minimum_jerk_profile(t)
        ts.append(min(1.0, max(0.0, w + random.uniform(-0.01, 0.01))))
    ts.sort()
    return ts

def _curved_path(p0, p1, curve_strength: float):
    x0,y0 = p0; x1,y1 = p1
    dx,dy = (x1-x0),(y1-y0)
    dist = math.hypot(dx,dy) or 1.0
    ang = math.atan2(dy,dx)
    nx, ny = -math.sin(ang), math.cos(ang)
    amp = curve_strength * min(180, max(30, dist*0.25)) * (1 if random.random()<0.5 else -1)
    c1 = (x0 + dx*0.33 + nx*random.uniform(0.2,0.6)*amp,
          y0 + dy*0.33 + ny*random.uniform(0.2,0.6)*amp)
    c2 = (x0 + dx*0.66 + nx*random.uniform(0.2,0.6)*amp,
          y0 + dy*0.66 + ny*random.uniform(0.2,0.6)*amp)
    return c1, c2, dist

def _bezier(p0, p1, p2, p3, t):
    u = 1-t
    return (
        (u**3)*p0[0] + 3*(u**2)*t*p1[0] + 3*u*(t**2)*p2[0] + (t**3)*p3[0],
        (u**3)*p0[1] + 3*(u**2)*t*p1[1] + 3*u*(t**2)*p2[1] + (t**3)*p3[1],
    )

def _fitts_duration(distance: float, target_w: float) -> float:
    a = 0.06
    b = 0.09
    return a + b * math.log2(distance / max(6.0, target_w) + 1.0)

def human_like_mouse_move(page, locator: Locator,
                                 overshoot_prob: float = 0.3,
                                 curve_strength: float = 0.4,
                                 settle_jitter: bool = True) -> bool:
    try:
        locator = locator.first
    except Exception:
        pass

    def _box():
        try:
            return locator.bounding_box()
        except Exception:
            return None

    def _in_view(box) -> bool:
        if not box:
            return False
        vp_h = _viewport_height(page)
        top = box["y"]
        bottom = box["y"] + box["height"]
        return (top >= 0) and (bottom <= vp_h)

    def _screen_to_page_xy(mx: float, my: float) -> Tuple[float, float]:
        m = _get_window_metrics(page)
        cx = (mx - m["sx"] - m["borderX"]) + m["vx"]
        cy = (my - m["sy"] - m["borderTop"]) + m["vy"]
        return cx, cy

    while True:
        box = _box()
        if box and _in_view(box):
            break
        ok_scroll = human_like_scroll(page, target=locator, max_steps=40)
        if not ok_scroll:
            box = _box()
            if box and _in_view(box):
                break
        _sleep_ms(120, 360)

    box = _box()
    if not box:
        return False
    tx, ty, target_w = _box_to_screen_xy(page, box)
    try:
        sx, sy = pyautogui.position()
    except Exception:
        return False

    start = (sx, sy)
    end = (tx, ty)
    dist = math.hypot(end[0]-start[0], end[1]-start[1])

    do_overshoot = (dist > 140) and (random.random() < overshoot_prob)
    end1 = (tx + random.uniform(-40, 40), ty + random.uniform(-40, 40)) if do_overshoot else end

    T1 = _fitts_duration(math.hypot(end1[0]-sx, end1[1]-sy), target_w)
    ts = _ease_samples(T1)
    c1, c2, _ = _curved_path(start, end1, curve_strength)
    for t in ts:
        x, y = _bezier(start, c1, c2, end1, t)
        pyautogui.moveTo(x, y, duration=0)
        _sleep_ms(4, 8)

    _sleep_ms(40, 120)

    if do_overshoot:
        sx2, sy2 = pyautogui.position()
        start2 = (sx2, sy2)
        end2 = end
        T2 = _fitts_duration(math.hypot(end2[0]-sx2, end2[1]-sy2), target_w*1.2)
        ts2 = _ease_samples(T2)
        c1b, c2b, _ = _curved_path(start2, end2, curve_strength*0.5)
        for t in ts2:
            x, y = _bezier(start2, c1b, c2b, end2, t)
            pyautogui.moveTo(x, y, duration=0)
            _sleep_ms(3, 7)
        _sleep_ms(30, 90)

    if settle_jitter and random.random() < 0.6:
        for _ in range(random.randint(2, 4)):
            jx = tx + random.uniform(-2, 2)
            jy = ty + random.uniform(-2, 2)
            pyautogui.moveTo(jx, jy, duration=0)
            _sleep_ms(12, 28)

    while True:
        try:
            mx, my = pyautogui.position()
        except Exception:
            break
        cx, cy = _screen_to_page_xy(mx, my)
        box = _box()
        if box and (box["x"] <= cx <= box["x"] + box["width"]) and (box["y"] <= cy <= box["y"] + box["height"]):
            return True
        tx, ty, _ = _box_to_screen_xy(page, box if box else {"x": cx, "y": cy, "width": 16, "height": 16})
        corr_x = tx + random.uniform(-1.5, 1.5)
        corr_y = ty + random.uniform(-1.5, 1.5)
        pyautogui.moveTo(corr_x, corr_y, duration=0)
        _sleep_ms(30, 80)

    return True


def human_like_click(page, locator):
    page.bring_to_front()
    _sleep_ms(*WAIT_M)
    ok = human_like_mouse_move(page, locator)
    if not ok:
        return
    if random.random() < 0.12:
        _sleep_ms(40, 120)
        pyautogui.moveRel(random.uniform(-2,2), random.uniform(-2,2), duration=0)
    pyautogui.click()
    _sleep_ms(50, 200)

def click_maybe_navigates(page, locator: Locator, *, may_navigate: bool = True, wait_until: str = "domcontentloaded"):
    try:
        locator = locator.first
    except Exception:
        pass
    if may_navigate:
        with page.expect_navigation(wait_until=wait_until, timeout=PAGE_LOAD_TIMEOUT_MS):
            human_like_click(page, locator)
    else:
        human_like_click(page, locator)
    wait_stable(page)

def simulation_type(element, text):
    for word in re.split(r"\s+", text.strip()):
        if not word:
            pyautogui.typewrite(" ", interval=random.uniform(0.08, 0.22))
            continue
        for ch in word:
            pyautogui.typewrite(ch, interval=random.uniform(0.05, 0.18))
            _sleep_ms(30, 120)
        pyautogui.typewrite(" ", interval=random.uniform(0.20, 0.90))

def _viewport_height(page):
    vp = page.viewport_size
    if vp:
        return vp["height"]
    return safe_evaluate(page, "() => window.innerHeight")

def _rand(a, b):
    return random.uniform(a, b)

def human_like_scroll(page, target: Optional[Locator] = None, *, max_steps: int = 40) -> bool:
    page.bring_to_front()
    def _ensure_cursor_in_window():
        try:
            m = _get_window_metrics(page)

            vp = page.viewport_size
            if vp and vp.get("width") and vp.get("height"):
                innerW, innerH = vp["width"], vp["height"]
            else:
                innerW = safe_evaluate(page, "() => window.innerWidth") or 800
                innerH = safe_evaluate(page, "() => window.innerHeight") or 600

            left = m["sx"] + m["borderX"]
            top = m["sy"] + m["borderTop"]
            right = left + innerW
            bottom = top + innerH

            mx, my = pyautogui.position()
            if not (left <= mx <= right and top <= my <= bottom):
                cx = left + innerW / 2.0
                cy = top + innerH / 2.0
                pyautogui.moveTo(cx, cy, duration=0)
                _sleep_ms(60, 140)
        except Exception:
            pass

    def _box():
        try:
            return target.first.bounding_box() if target else None
        except Exception:
            try:
                return target.bounding_box() if target else None
            except Exception:
                return None

    def _in_view(box) -> bool:
        if not box:
            return False
        vp_h = _viewport_height(page)
        top = box["y"]
        bottom = box["y"] + box["height"]
        return (top >= 0) and (bottom <= vp_h)

    if target is None:
        for _ in range(random.randint(2, 6)):
            delta_y = random.choice([_rand(50, 150), _rand(-120, -60), _rand(300, 600)])
            _ensure_cursor_in_window()
            pyautogui.scroll(int(-delta_y))
            _sleep_ms(300, 1200)

            if random.random() < 0.22:
                _ensure_cursor_in_window()
                pyautogui.scroll(int(delta_y * random.uniform(0.25, 0.6)))
                _sleep_ms(400, 1200)
        return True

    try:
        target = target.first
    except Exception:
        pass

    def _wheel(step):
        noisy = step + random.uniform(-25, 25)
        _ensure_cursor_in_window()
        pyautogui.scroll(int(-noisy))
        _sleep_ms(180, 520)

    vp_h = _viewport_height(page)
    misses = 0
    for _ in range(max_steps):
        box = _box()
        if not box:
            _wheel(_rand(220, 520))
            misses += 1
            if misses >= 6 and random.random() < 0.35:
                _wheel(_rand(-280, -140))
            continue

        top = box["y"]
        bottom = box["y"] + box["height"]
        center_y = top + box["height"] / 2

        if _in_view(box):
            target_screen_y = vp_h * _rand(0.55, 0.70)
            delta = center_y - target_screen_y
            if abs(delta) > 8:
                steps_needed = max(1, int(abs(delta) / _rand(100, 180)))
                per_step = max(20, min(240, delta / steps_needed))
                for _ in range(steps_needed):
                    _wheel(per_step)
                    if random.random() < 0.18:
                        _wheel(-per_step * _rand(0.15, 0.35))
            if random.random() < 0.6:
                _wheel(_rand(-40, 40))
                _sleep_ms(90, 240)
            break

        if bottom < 0:
            step = -_rand(180, 420)
        elif top > vp_h:
            step = _rand(200, 460)
        else:
            step = _rand(-160, 160)

        _wheel(step)
        if random.random() < 0.2:
            _wheel(step * _rand(0.4, 0.8))
            _sleep_ms(140, 380)
            _wheel(-step * _rand(0.2, 0.5))

    try:
        box = _box()
        if box and _in_view(box):
            if random.random() < 0.5:
                _ensure_cursor_in_window()
                pyautogui.scroll(int(-_rand(-35, 35)))
                _sleep_ms(*WAIT_S)
            return True
    except Exception:
        pass
    return False


def human_like_sleep(min_ms=100, max_ms=500):
    sleep(random.uniform(min_ms / 1000, max_ms / 1000))

def start_profile(profile_id, profiles_folder):
    c = 0
    while True:
        try:
            r = requests.get(
                f"https://launcher.mlx.yt:45001/api/v2/profile/f/{profiles_folder}/p/{profile_id}/start?automation_type=playwright",
                headers=HEADERS, timeout=20
            )
            return r.json()['data']['port']
        except:
            c += 1
            if c == 5:
                return None
            sleep(2)

def go_to_ad(page, question):
    if len(question['page'].split('/')) == 4:
        url = '/'.join(question['page'].split('/')[:-1])
        img = page.locator(f'a[href="{url}/"]')
        human_like_scroll(page, img)
        human_like_sleep(*WAIT_S)
        click_maybe_navigates(page, img, may_navigate=True)
        human_like_sleep(*WAIT_M)
        name = question["page"].split("/")[-1]
        loc = page.locator(f'a[href="/{name}/"]')
        human_like_scroll(page, loc)
        human_like_sleep(*WAIT_S)
        click_maybe_navigates(page, loc, may_navigate=True)
        human_like_sleep(*WAIT_M)
    else:
        img = page.locator(f'a[href$="{question["page"]}/"]')
        human_like_scroll(page, img)
        human_like_sleep(*WAIT_S)
        click_maybe_navigates(page, img, may_navigate=True)
    human_like_sleep(*WAIT_M)
    if 'Forbidden' in page.content():
        return [False, 8]
    n = question['n']
    for i in range(7, n, 7):
        locator = page.locator(f'a[href$="/{(i - 1)*20}/"]:text-is("{i}")').first
        human_like_scroll(page, locator)
        human_like_sleep(*WAIT_S)
        click_maybe_navigates(page, locator, may_navigate=True)
        human_like_sleep(*WAIT_M)
    if n != 1:
        locator = page.locator(f'a[href$="/{(n - 1)*20}/"]:text-is("{n}")').first
        human_like_scroll(page, locator)
        human_like_sleep(*WAIT_S)
        click_maybe_navigates(page, locator, may_navigate=True)
        human_like_sleep(*WAIT_M)
    name = question["link"].split('/')[-1]
    c_ = 0
    while True:
        if page.locator(f'a[href$="{name}"]').count() > 0:
            break
        if c_ == 5:
            page.goto(question['link'])
            human_like_sleep(*WAIT_M)
            human_like_scroll(page)
            human_like_sleep(*WAIT_M)
            return True
        c_ += 1
        n += 1
        locator = page.locator(f'a[href$="/{(n - 1)*20}/"]:text-is("{n}")').first
        human_like_scroll(page, locator)
        human_like_sleep(*WAIT_S)
        click_maybe_navigates(page, locator, may_navigate=True)
        human_like_sleep(*WAIT_M)
    img = page.locator(f'a[href$="{name}"]').last
    human_like_scroll(page, img)
    human_like_sleep(*WAIT_S)
    click_maybe_navigates(page, img, may_navigate=True)
    human_like_sleep(*WAIT_M)
    human_like_scroll(page)
    human_like_sleep(*WAIT_M)
    return True

def win_arrow(direction="up", times=1, delay=0.15):
    for _ in range(times):
        pyautogui.hotkey("win", direction)
        time.sleep(delay)

def spam(profile_data, ad_data, profiles_folder):
    port = start_profile(profile_data['profile_id'], profiles_folder)
    if not port:
        return [False, 5]
    try:
        with sync_playwright() as p:
            browser = p.chromium.connect_over_cdp(f'http://127.0.0.1:{port}')
            context = browser.contexts[0] if browser.contexts else browser.new_context()
            page = context.pages[0] if context.pages else context.new_page()
            page.goto('https://www.bazos.sk', timeout=PAGE_LOAD_TIMEOUT_MS)
            human_like_sleep(*WAIT_M)
            page.bring_to_front()
            win_arrow("up", times=2)
            if ad_data['n'] == -1:
                page.goto(ad_data['link'], timeout=PAGE_LOAD_TIMEOUT_MS)
            else:
                if not go_to_ad(page, ad_data):
                    return [False, 8]
            page.wait_for_selector('body', timeout=PAGE_LOAD_TIMEOUT_MS)
            has_form = (page.locator("[name='mailo']").count()) > 0 and (page.locator("[name='texto']").count()) > 0
            if has_form:
                mail_input = page.wait_for_selector("[name='mailo']", timeout=PAGE_LOAD_TIMEOUT_MS)
                human_like_scroll(page, mail_input)
                human_like_sleep(*WAIT_S)
                human_like_mouse_move(page, mail_input)
                human_like_sleep(*WAIT_S)
                human_like_click(page, mail_input)
                human_like_sleep(*WAIT_M)
                simulation_type(mail_input, profile_data['email'])
                text_input = page.wait_for_selector("[name='texto']", timeout=PAGE_LOAD_TIMEOUT_MS)
                human_like_scroll(page, text_input)
                human_like_sleep(*WAIT_S)
                human_like_mouse_move(page, text_input)
                human_like_sleep(*WAIT_S)
                human_like_click(page, text_input)
                human_like_sleep(*WAIT_M)
                simulation_type(text_input, ad_data['question'])
                send_btn = page.wait_for_selector('#mailbutton', timeout=PAGE_LOAD_TIMEOUT_MS)
                human_like_scroll(page)
                human_like_sleep(*WAIT_M)
                human_like_scroll(page, send_btn)
                human_like_sleep(*WAIT_M)
                human_like_mouse_move(page, send_btn)
                human_like_sleep(*WAIT_M)
                overlay = page.locator('#overlaymail')
                before_html = overlay.inner_html()
                with page.expect_response(lambda r: 'ad-mail.php' in r.url):
                    click_maybe_navigates(page, send_btn, may_navigate=False)
                page.wait_for_function(
                    """prev => {
                        const el = document.querySelector('#overlaymail');
                        return el && el.innerHTML !== prev;
                    }""",
                    arg=before_html,
                    timeout=PAGE_LOAD_TIMEOUT_MS,
                )
                human_like_sleep(*WAIT_M)
                title = page.locator('#overlaymail b').inner_text().strip()
                if 'Zablokovaný mail' in title:
                    return [False, 3]
                elif "Odoslanie E-mailu" not in title:
                    try:
                        human_like_scroll(page, send_btn)
                        human_like_sleep(*WAIT_M)
                        human_like_mouse_move(page, send_btn)
                        human_like_sleep(*WAIT_M)
                        overlay = page.locator('#overlaymail')
                        before_html = overlay.inner_html()
                        with page.expect_response(lambda r: 'ad-mail.php' in r.url):
                            click_maybe_navigates(page, send_btn, may_navigate=False)
                        page.wait_for_function(
                            """prev => {
                                const el = document.querySelector('#overlaymail');
                                return el && el.innerHTML !== prev;
                            }""",
                            arg=before_html,
                            timeout=PAGE_LOAD_TIMEOUT_MS,
                        )
                        human_like_sleep(*WAIT_M)
                        title = page.locator('#overlaymail b').inner_text().strip()
                    except:
                        pass
                message = page.eval_on_selector('#overlaymail', """
                el => {
                  const copy = el.cloneNode(true);
                  const b = copy.querySelector('b');
                  if (b) b.remove();
                  return copy.innerText.trim();
                }
                """)
                return [True, 0, message]
            elif (page.get_by_text("Vaše telefónne číslo", exact=False).count()) > 0:
                return [False, 2]
            else:
                return [False, 6]
    except (PlaywrightTimeoutError, TimeoutError):
        return [False, 1, 'что то выполнялось слишком долго']
    except Exception as e:
        return [False, 4, e]

def _spam_runner(profile_data, ad_data, profiles_folder, q: Queue):
    try:
        res = spam(profile_data, ad_data, profiles_folder)
    except Exception as e:
        res = [False, 4, e]
    try:
        q.put(res)
    except Exception:
        pass

def spam_with_timeout_proc(profile_data, ad_data, profiles_folder, timeout_s=SPAM_TIMEOUT_S):
    q = Queue()
    p = Process(target=_spam_runner, args=(profile_data, ad_data, profiles_folder, q), daemon=True)
    p.start()
    p.join(timeout_s)
    if p.is_alive():
        try:
            requests.get(
                f"https://launcher.mlx.yt:45001/api/v1/profile/stop/p/{profile_data['profile_id']}",
                headers=HEADERS, timeout=10
            )
        except Exception:
            pass
        p.terminate()
        p.join(5)
        return [False, 7]
    try:
        return q.get_nowait()
    except QueueEmpty:
        return [False, 4, "какая то проблема в основном цикле спама"]

def now_msk():
    return datetime.now(MSK)

def next_after(first: datetime) -> datetime:
    """
    Сделать переменную в бд
      - 0–2 дней → запускать раз в сутки
      - 2–7 дней → раз в 12 часов
      - >7 дней  → раз в 2 часа
    """
    now = now_msk()
    age = now - first
    if age <= timedelta(days=2):
        return now + timedelta(hours=24)
    elif age <= timedelta(days=5):
        return now + timedelta(hours=12)
    else:
        return now + timedelta(hours=2)

def main():
    global main_settings
    while True:
        main_settings = db.settings.find_one({'name': 'main_settings'})
        profile_data = db.profiles.find_one_and_update(
            {"status": "new"},
            {"$set": {"status": "reserved"}},
            sort=[("next", 1)],
            return_document=ReturnDocument.AFTER
        )

        if not profile_data:
            log("нет доступных профилей, ожидаю")
            sleep(10)
            continue

        log(f"за агентом закреплён профиль {profile_data['name']} ({profile_data['profile_id']})")

        next_time = profile_data.get("next")
        if next_time:
            wait_sec = (next_time - now_msk()).total_seconds()
            if wait_sec > 0:
                log(f"ожидаю {wait_sec} секунд до начала", level="main")
                sleep(wait_sec)

        while True:
            ad_data = db.advertisements.find_one_and_update(
                {"status": "new"},
                {"$set": {"status": "in_work"}},
                sort=[("_id", -1)],
                return_document=ReturnDocument.AFTER
            )
            if not ad_data:
                log("нет доступных объявлений, ожидаю")
                sleep(10)
                continue
            else:
                log(f"агент взял в работу объявление {ad_data['name']}")
                n = get_page_number(ad_data['page'], ad_data['n'], ad_data['name'])
                if n is False:
                    log('объявление не найдено на ближайших 28 страницах, перехожу по прямой ссылке', level="main")
                    ad_data['n'] = -1
                else:
                    ad_data['n'] = n
                break

        db.profiles.update_one(
            {"_id": ObjectId(profile_data['_id'])},
            {"$set": {"status": "in_work"}}
        )

        spam_start_dt = now_msk()
        start = perf_counter()
        spam_result = spam_with_timeout_proc(profile_data, ad_data, main_settings['items']['profiles_folder']['value'])

        # Успех / номер ошибки (0 если её нет)
        # 1 - ошибка по времени
        # 2 - умерли куки
        # 3 - заблокирована почта
        # 4 - Неизвестная ошибка
        # 5 - профиль не запущен
        # 6 - проблема с формой
        # 7 - пограмма зависла
        # 8 - проблема с ip

        if spam_result[0] == True:
            now = now_msk()
            first = profile_data.get("first") or now
            new_next = next_after(first)

            db.profiles.update_one(
                {"_id": ObjectId(profile_data['_id'])},
                {"$set": {
                    "status": "new",
                    "first": first,
                    "next": new_next
                }}
            )

            db.advertisements.update_one(
                {"_id": ObjectId(ad_data['_id'])},
                {"$set": {
                    "status": "old",
                    "profile_id": profile_data['_id'],
                    "agent": SERVER_NAME
                }}
            )

            elapsed = perf_counter() - start
            ad_created_dt = ad_data['_id'].generation_time.astimezone(MSK)

            log(f"успешно, выполнилось за {elapsed:.1f} сек\n{spam_result[2]}\n({ad_created_dt:%H:%M} | {spam_start_dt:%H:%M})",level="main")
        else:
            err = spam_result[1]
            if err == 1:
                db.profiles.update_one(
                    {"_id": ObjectId(profile_data['_id'])},
                    {"$set": {"status": "new"}}
                )
                db.advertisements.update_one(
                    {"_id": ObjectId(ad_data['_id'])},
                    {"$set": {"status": "new"}}
                )
                log("ошибка времени, профиль и объявление разблокированы", level="main")
            elif err == 2:
                db.profiles.delete_one(
                    {"_id": ObjectId(profile_data['_id'])}
                )
                db.advertisements.update_one(
                    {"_id": ObjectId(ad_data['_id'])},
                    {"$set": {"status": "new"}}
                )
                payload = {"ids": [profile_data['profile_id']], "permanently": True}
                requests.post(
                    "https://api.multilogin.com/profile/remove",
                    headers=HEADERS,
                    json=payload,
                    timeout=10
                )
                log("умерли куки, объявление разблокировано, профиль удален", level="main")
            elif err == 3:
                db.profiles.update_one(
                    {"_id": ObjectId(profile_data['_id'])},
                    {"$set": {"status": "new_email"}}
                )
                db.advertisements.update_one(
                    {"_id": ObjectId(ad_data['_id'])},
                    {"$set": {"status": "new"}}
                )
                log("почта заблокирована, профиль ожидает замены почты", level="main")
            elif err == 4:
                if "ERR_TIMED_OUT" in str(spam_result[2]):
                    postpone = now_msk() + timedelta(minutes=30)
                    db.profiles.update_one(
                        {"_id": ObjectId(profile_data['_id'])},
                        {"$set": {
                            "status": "new",
                            "next": postpone
                        }}
                    )
                    db.advertisements.update_one(
                        {"_id": ObjectId(ad_data['_id'])},
                        {"$set": {"status": "new"}}
                    )
                    log(f"ошибка загрузки главной страницы, профиль отложен на 30 минут", level="main")
                else:
                    db.profiles.update_one(
                        {"_id": ObjectId(profile_data['_id'])},
                        {"$set": {"status": "new"}}
                    )
                    db.advertisements.update_one(
                        {"_id": ObjectId(ad_data['_id'])},
                        {"$set": {"status": "new"}}
                    )
                    log(f"неизвестная ошибка {spam_result[2]}, профиль и объявление разблокированы", level="main")
            elif err == 5:
                postpone = now_msk() + timedelta(minutes=5)
                db.profiles.update_one(
                    {"_id": ObjectId(profile_data['_id'])},
                    {"$set": {
                        "status": "new",
                        "next": postpone
                    }}
                )
                db.advertisements.update_one(
                    {"_id": ObjectId(ad_data['_id'])},
                    {"$set": {"status": "new"}}
                )
                log(f"профиль не запустился с 5 попыток, отложен на 5 минут, объявление разблокировано")

            elif err == 6:
                db.profiles.update_one(
                    {"_id": ObjectId(profile_data['_id'])},
                    {"$set": {"status": "new"}}
                )
                db.advertisements.update_one(
                    {"_id": ObjectId(ad_data['_id'])},
                    {"$set": {"status": "old"}}
                )
                log("какие-то проблемы с формой, вопрос удалён, профиль разблокирован", level="main")
            elif err == 7:
                db.profiles.update_one(
                    {"_id": ObjectId(profile_data['_id'])},
                    {"$set": {"status": "new"}}
                )
                db.advertisements.update_one(
                    {"_id": ObjectId(ad_data['_id'])},
                    {"$set": {"status": "new"}}
                )
                log("программа зависла, перезапуск", level="main")
            elif err == 8:
                postpone = now_msk() + timedelta(minutes=30)
                db.profiles.update_one(
                    {"_id": ObjectId(profile_data['_id'])},
                    {"$set": {
                        "status": "new",
                        "next": postpone
                    }}
                )
                db.advertisements.update_one(
                    {"_id": ObjectId(ad_data['_id'])},
                    {"$set": {"status": "new"}}
                )
                log("ip заблочен, профиль заблокирован на 30 минут", level="main")
            else:
                db.profiles.update_one(
                    {"_id": ObjectId(profile_data['_id'])},
                    {"$set": {"status": "new"}}
                )
                db.advertisements.update_one(
                    {"_id": ObjectId(ad_data['_id'])},
                    {"$set": {"status": "new"}}
                )
                log(f"неизвестная ошибка", level="main")

        requests.get(
            f"https://launcher.mlx.yt:45001/api/v1/profile/stop/p/{profile_data['profile_id']}",
            headers=HEADERS,
            timeout=20
        )

if __name__ == "__main__":
    global SERVER_NAME
    with open("serv_name.txt", "r", encoding="utf-8") as f:
        SERVER_NAME = f.readline().strip()
    main()