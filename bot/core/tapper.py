import asyncio
import random
import os
import json
import aiohttp
import aiofiles
import functools
import traceback

from typing import Callable, Tuple
from time import time
from urllib.parse import unquote, quote
from datetime import datetime, timedelta
from aiocfscrape import CloudflareScraper
from aiohttp_proxy import ProxyConnector
from better_proxy import Proxy
from pyrogram import Client
from pyrogram.errors import Unauthorized, UserDeactivated, AuthKeyUnregistered, FloodWait
from pyrogram.raw.functions.messages import RequestAppWebView
from pyrogram.raw.functions import account
from pyrogram.raw.types import InputBotAppShortName, InputNotifyPeer, InputPeerNotifySettings
from .headers import headers
from .agents import generate_random_user_agent
from bot.utils import logger
from bot.config import settings
from bot.exceptions import InvalidSession
from bot.utils.connection_manager import connection_manager

def error_handler(func: Callable):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        try:
            return await func(*args, **kwargs)
        except Exception as e:
            await asyncio.sleep(1)

    return wrapper


class Tapper:
    def __init__(self, tg_client: Client, proxy: str):
        self.tg_client = tg_client
        self.session_name = tg_client.name
        self.proxy = proxy
        self.tg_web_data = None
        self.tg_client_id = 0

        self.user_agents_dir = "user_agents"
        self.session_ug_dict = {}
        self.headers = headers.copy()

        self.last_checkin_time = None
        self.last_checkin_day = None

    async def init(self):
        os.makedirs(self.user_agents_dir, exist_ok=True)
        await self.load_user_agents()
        user_agent, sec_ch_ua = await self.check_user_agent()
        self.headers['User-Agent'] = user_agent
        self.headers['Sec-Ch-Ua'] = sec_ch_ua

    async def generate_random_user_agent(self):
        user_agent, sec_ch_ua = generate_random_user_agent(device_type='android', browser_type='webview')
        return user_agent, sec_ch_ua

    async def load_user_agents(self) -> None:
        try:
            os.makedirs(self.user_agents_dir, exist_ok=True)
            filename = f"{self.session_name}.json"
            file_path = os.path.join(self.user_agents_dir, filename)

            if not os.path.exists(file_path):
                logger.info(f"{self.session_name} | User agent file not found. A new one will be created when needed.")
                return

            try:
                async with aiofiles.open(file_path, 'r') as user_agent_file:
                    content = await user_agent_file.read()
                    if not content.strip():
                        logger.warning(f"{self.session_name} | User agent file '{filename}' is empty.")
                        return

                    data = json.loads(content)
                    if data['session_name'] != self.session_name:
                        logger.warning(f"{self.session_name} | Session name mismatch in file '{filename}'.")
                        return

                    self.session_ug_dict = {self.session_name: data}
            except json.JSONDecodeError:
                logger.warning(f"{self.session_name} | Invalid JSON in user agent file: {filename}")
            except Exception as e:
                logger.error(f"{self.session_name} | Error reading user agent file {filename}: {e}")
        except Exception as e:
            logger.error(f"{self.session_name} | Error loading user agents: {e}")

    async def save_user_agent(self) -> Tuple[str, str]:
        user_agent_str, sec_ch_ua = await self.generate_random_user_agent()

        new_session_data = {
            'session_name': self.session_name,
            'user_agent': user_agent_str,
            'sec_ch_ua': sec_ch_ua
        }

        file_path = os.path.join(self.user_agents_dir, f"{self.session_name}.json")
        try:
            async with aiofiles.open(file_path, 'w') as user_agent_file:
                await user_agent_file.write(json.dumps(new_session_data, indent=4, ensure_ascii=False))
        except Exception as e:
            logger.error(f"{self.session_name} | Error saving user agent data: {e}")

        self.session_ug_dict = {self.session_name: new_session_data}

        logger.info(f"{self.session_name} | User agent saved successfully: {user_agent_str}")

        return user_agent_str, sec_ch_ua

    async def check_user_agent(self) -> Tuple[str, str]:
        if self.session_name not in self.session_ug_dict:
            return await self.save_user_agent()

        session_data = self.session_ug_dict[self.session_name]
        if 'user_agent' not in session_data or 'sec_ch_ua' not in session_data:
            return await self.save_user_agent()

        return session_data['user_agent'], session_data['sec_ch_ua']

    @error_handler
    async def check_proxy(self, http_client: aiohttp.ClientSession) -> bool:
        try:
            response = await http_client.get(url='https://ipinfo.io/json', timeout=aiohttp.ClientTimeout(total=5))
            data = await response.json()

            ip = data.get('ip')
            city = data.get('city')
            country = data.get('country')

            logger.info(
                f"{self.session_name} | Check proxy! Country: <cyan>{country}</cyan> | City: <light-yellow>{city}</light-yellow> | Proxy IP: {ip}")

            return True

        except Exception as error:
            logger.error(f"{self.session_name} | Proxy error: {error}")
            return False

    @error_handler
    async def get_tg_web_data(self) -> str:

        if self.proxy:
            proxy = Proxy.from_str(self.proxy)
            proxy_dict = dict(
                scheme=proxy.protocol,
                hostname=proxy.host,
                port=proxy.port,
                username=proxy.login,
                password=proxy.password
            )
        else:
            proxy_dict = None

        self.tg_client.proxy = proxy_dict

        try:
            if not self.tg_client.is_connected:
                try:
                    await self.tg_client.connect()

                except (Unauthorized, UserDeactivated, AuthKeyUnregistered):
                    raise InvalidSession(self.session_name)

            while True:
                try:
                    peer = await self.tg_client.resolve_peer('realgoats_bot')
                    break
                except FloodWait as fl:
                    fls = fl.value

                    logger.warning(f"{self.session_name} | FloodWait {fl}")
                    wait_time = random.randint(3600, 12800)
                    logger.info(f"{self.session_name} | Sleep {wait_time}s")
                    await asyncio.sleep(wait_time)

            ref_id = random.choices([settings.REF_ID, "425d2a82-bfdf-44e9-a111-b9b0665a28ab"], weights=[75, 25], k=1)[0]
            web_view = await self.tg_client.invoke(RequestAppWebView(
                peer=peer,
                platform='android',
                app=InputBotAppShortName(bot_id=peer, short_name="run"),
                write_allowed=True,
                start_param=ref_id
            ))

            auth_url = web_view.url
            init_data = unquote(
                string=auth_url.split('tgWebAppData=', maxsplit=1)[1].split('&tgWebAppVersion', maxsplit=1)[0])

            me = await self.tg_client.get_me()
            self.tg_client_id = me.id

            if self.tg_client.is_connected:
                await self.tg_client.disconnect()

            return init_data

        except InvalidSession as error:
            raise error

        except Exception as error:
            logger.error(f"{self.session_name} | Unknown error: {error}")
            await asyncio.sleep(delay=3)

    async def make_request(self, http_client, method, url=None, **kwargs):
        try:
            response = await http_client.request(method, url, **kwargs)
            response.raise_for_status()
            response_json = await response.json()
            return response_json
        except aiohttp.ClientError as e:
            logger.error(f"{self.session_name} | Error request: {str(e)}")
            return None
        except json.JSONDecodeError:
            logger.error(f"{self.session_name} | Error decode JSON")
            return None

    @error_handler
    async def login(self, http_client, init_data):
        headers = http_client.headers.copy()
        headers['Rawdata'] = init_data
        try:
            response = await self.make_request(http_client, 'POST', url="https://dev-api.goatsbot.xyz/auth/login",
                                               json={}, headers=headers)
            return response
        except Exception as e:
            logger.error(f"{self.session_name} | Error in login: {str(e)}")
            return None

    @error_handler
    async def get_me_info(self, http_client):
        return await self.make_request(http_client, 'GET', url="https://api-me.goatsbot.xyz/users/me")

    @error_handler
    async def get_tasks(self, http_client: aiohttp.ClientSession) -> dict:
        return await self.make_request(http_client, 'GET', url='https://api-mission.goatsbot.xyz/missions/user')

    @error_handler
    async def done_task(self, http_client: aiohttp.ClientSession, task_id: str):
        return await self.make_request(http_client, 'POST',
                                       url=f'https://dev-api.goatsbot.xyz/missions/action/{task_id}')

    @error_handler
    async def daily_checkin(self, http_client: aiohttp.ClientSession):
        checkin_info = await self.make_request(http_client, 'GET', url='https://api-checkin.goatsbot.xyz/checkin/user')

        if checkin_info is None:
            logger.error(f"{self.session_name} | Response: None")
            return

        if not isinstance(checkin_info, dict):
            logger.error(
                f"{self.session_name} | Unexpected response from server. Type: {type(checkin_info)}")
            return

        checkin_result = checkin_info.get('result', [])
        last_checkin_time = checkin_info.get('lastCheckinTime', 0)

        last_checkin_datetime = datetime.fromtimestamp(last_checkin_time / 1000)
        current_time = datetime.now()
        time_since_last_checkin = current_time - last_checkin_datetime

        formatted_last_checkin = last_checkin_datetime.strftime("%d.%m.%Y %H:%M")

        if not checkin_result:
            logger.warning(f"{self.session_name} | List checkins is none")
            return

        if time_since_last_checkin < timedelta(hours=26):
            next_checkin_time = last_checkin_datetime + timedelta(hours=26)
            time_left = next_checkin_time - current_time
            hours, remainder = divmod(time_left.seconds, 3600)
            minutes, _ = divmod(remainder, 60)
            logger.info(f"{self.session_name} | Last check-in: {formatted_last_checkin} | Time to next check-in {hours:02d} hour(s) {minutes:02d} minute(s)")
            return

        available_checkin = next((checkin for checkin in checkin_result if not checkin['status']), None)

        if available_checkin is None:
            logger.info(f"{self.session_name} | Not checkins")
            return

        day = available_checkin['day']
        reward = available_checkin['reward']
        checkin_id = available_checkin['_id']

        logger.info(
            f"{self.session_name} | Day: <light-yellow>{day}</light-yellow> | Reward: <light-yellow>{reward}</light-yellow>")


        await asyncio.sleep(random.uniform(15, 30))

        checkin_action_response = await self.make_request(http_client, 'POST',
                                                          url=f'https://api-checkin.goatsbot.xyz/checkin/action/{checkin_id}')

        if checkin_action_response and checkin_action_response.get('status') == 'success':
            logger.info(f"{self.session_name} | Successfully received daily check-in: <green>+{reward}</green>")
            self.last_checkin_time = current_time
            self.last_checkin_day = day
        else:
            logger.error(
                f"{self.session_name} | Error: {checkin_action_response}")

        return checkin_action_response

    async def run(self) -> None:
        if settings.USE_RANDOM_DELAY_IN_RUN:
            random_delay = random.randint(settings.RANDOM_DELAY_IN_RUN[0], settings.RANDOM_DELAY_IN_RUN[1])
            logger.info(
                f"{self.session_name} | The Bot will go live in <y>{random_delay}s</y>")
            await asyncio.sleep(random_delay)

        await self.init()

        proxy_conn = ProxyConnector().from_url(self.proxy) if self.proxy else None
        http_client = CloudflareScraper(headers=self.headers, connector=proxy_conn)
        connection_manager.add(http_client)

        if settings.USE_PROXY:
            if not self.proxy:
                logger.error(f"{self.session_name} | Proxy is not set. Aborting operation.")
                return
            if not await self.check_proxy(http_client):
                logger.error(f"{self.session_name} | Proxy check failed. Aborting operation.")
                return

        init_data = await self.get_tg_web_data()

        while True:
            try:
                if http_client.closed:
                    if proxy_conn:
                        if not proxy_conn.closed:
                            await proxy_conn.close()

                    proxy_conn = ProxyConnector().from_url(self.proxy) if self.proxy else None
                    http_client = CloudflareScraper(headers=self.headers, connector=proxy_conn)
                    connection_manager.add(http_client)

                    init_data = await self.get_tg_web_data()

                _login = await self.login(http_client=http_client, init_data=init_data)

                if _login is None:
                    logger.error(f"{self.session_name} | Login None. Check connection ...")
                    try:
                        await self.check_proxy(http_client)
                        logger.info(
                            f"{self.session_name} | Connection is good. Maybe error in back server?")
                    except Exception as e:
                        logger.error(f"{self.session_name} | Error in connection: {str(e)}")

                    logger.info(f"{self.session_name} | Retry with 6 hours.")
                    await asyncio.sleep(21000)
                    continue

                accessToken = _login.get('tokens', {}).get('access', {}).get('token')
                if not accessToken:
                    logger.info(f"{self.session_name} | 🐐 <lc>Error with received token</lc>")
                    await asyncio.sleep(3600)
                    logger.info(f"{self.session_name} | Sleep <lc>3600s</lc>")
                    continue

                http_client.headers['Authorization'] = f'Bearer {accessToken}'
                self.headers['Authorization'] = f'Bearer {accessToken}'
                me_info = await self.get_me_info(http_client=http_client)
                if me_info is None:
                    logger.error(f"{self.session_name} | Error while get information on user")
                    continue

                logger.info(f"{self.session_name} | 🐐 <lc>Login successfully</lc> | Age: <lc>{me_info.get('age', 'Н/Д')}</lc> | Balance: <lc>{me_info.get('balance', 'Н/Д'):,}</lc>")

                await self.daily_checkin(http_client=http_client)

                tasks = await self.get_tasks(http_client=http_client)
                if tasks is None:
                    logger.error(f"{self.session_name} | Error while get user tasks")
                    continue

                for project, project_tasks in tasks.items():
                    if not isinstance(project_tasks, list):
                        logger.warning(f"{self.session_name} | Unknown error with tasks {project}")
                        continue

                    for task in project_tasks:
                        if not isinstance(task, dict):
                            logger.warning(f"{self.session_name} | Unknown error with tasks {project}")
                            continue

                        if not task.get('status'):
                            task_id = task.get('_id')
                            task_name = task.get('name')
                            task_reward = task.get('reward')

                            if not all([task_id, task_name, task_reward]):
                                logger.warning(
                                    f"{self.session_name} | Unknown error with tasks {project}")
                                continue

                            done_result = await self.done_task(http_client=http_client, task_id=task_id)

                            if done_result and done_result.get('status') == 'success':
                                logger.info(
                                    f"{self.session_name} | Tasks successfully completed: {project}: {task_name} | Reward: <green>+{task_reward}</green>")
                            else:
                                logger.warning(f"Unknown error with tasks: {project}: {task_name}")

                        await asyncio.sleep(random.randint(3,15))

            except aiohttp.ClientConnectorError as error:
                delay = random.randint(1800, 3600)
                logger.error(f"{self.session_name} | Connection error: {error}. Retrying in {delay} seconds.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                await asyncio.sleep(delay)


            except aiohttp.ServerDisconnectedError as error:
                delay = random.randint(900, 1800)
                logger.error(f"{self.session_name} | Server disconnected: {error}. Retrying in {delay} seconds.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                await asyncio.sleep(delay)


            except aiohttp.ClientResponseError as error:
                delay = random.randint(3600, 7200)
                logger.error(
                    f"{self.session_name} | HTTP response error: {error}. Status: {error.status}. Retrying in {delay} seconds.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                await asyncio.sleep(delay)


            except aiohttp.ClientError as error:
                delay = random.randint(3600, 7200)
                logger.error(f"{self.session_name} | HTTP client error: {error}. Retrying in {delay} seconds.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                await asyncio.sleep(delay)


            except asyncio.TimeoutError:
                delay = random.randint(7200, 14400)
                logger.error(f"{self.session_name} | Request timed out. Retrying in {delay} seconds.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                await asyncio.sleep(delay)


            except InvalidSession as error:
                logger.critical(f"{self.session_name} | Invalid Session: {error}. Manual intervention required.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                raise error


            except json.JSONDecodeError as error:
                delay = random.randint(1800, 3600)
                logger.error(f"{self.session_name} | JSON decode error: {error}. Retrying in {delay} seconds.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                await asyncio.sleep(delay)

            except KeyError as error:
                delay = random.randint(1800, 3600)
                logger.error(
                    f"{self.session_name} | Key error: {error}. Possible API response change. Retrying in {delay} seconds.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                await asyncio.sleep(delay)


            except Exception as error:
                delay = random.randint(7200, 14400)
                logger.error(f"{self.session_name} | Unexpected error: {error}. Retrying in {delay} seconds.")
                logger.debug(f"Full error details: {traceback.format_exc()}")
                await asyncio.sleep(delay)

            finally:
                await http_client.close()
                if proxy_conn:
                    if not proxy_conn.closed:
                        await proxy_conn.close()
                connection_manager.remove(http_client)

                sleep_time = random.randint(settings.SLEEP_TIME[0], settings.SLEEP_TIME[1])
                hours = int(sleep_time // 3600)
                minutes = (int(sleep_time % 3600)) // 60
                logger.info(
                    f"{self.session_name} | Sleep <yellow>{hours} hours</yellow> and <yellow>{minutes} minutes</yellow>")
                await asyncio.sleep(sleep_time)

async def run_tapper(tg_client: Client, proxy: str | None):
    session_name = tg_client.name
    if settings.USE_PROXY and not proxy:
        logger.error(f"{session_name} | No proxy found for this session")
        return
    try:
        await Tapper(tg_client=tg_client, proxy=proxy).run()
    except InvalidSession:
        logger.error(f"{session_name} | Invalid Session")
