import pymysql.cursors
import vk_api.vk_api
from vk_api.bot_longpoll import VkBotLongPoll, VkBotEventType
from vk_api.keyboard import VkKeyboard, VkKeyboardColor
import requests
from bs4 import BeautifulSoup
import threading
from threading import Thread

my_host = 'tosha582.beget.tech'
my_user = 'tosha582_bot'
my_pas = 'A1519582a'
my_db = 'tosha582_bot'
token = '2f4ebb55b097bca9da6a516fa6c733016e9fe08fc100506fb8248288d9acf61e8e6349277fc80667140e0'

vk = vk_api.VkApi(token=token)
long_poll = VkBotLongPoll(vk, 107146800)
vk = vk.get_api()
lock_users = set()
deleted_groups = set()


def send_message(aim_id, text):
    vk.messages.send(peer_id=aim_id, message=text, random_id=0, attachment=None)


def return_base(table, get_request):
    try:
        connection = pymysql.connect(host=my_host, user=my_user, password=my_pas, db=my_db, charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        array = []
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {table}")
            array = [el.get(get_request) for el in cursor]
    finally:
        connection.close()
        return array


def add_group(group_id, title):
    try:
        connection = pymysql.connect(host=my_host, user=my_user, password=my_pas, db=my_db, charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        with connection.cursor() as cursor:
            cursor.execute(f"INSERT INTO groups VALUES ({group_id}, '{title}')")
            connection.commit()
            send_message(group_id, "Беседа добавлена в БД")
    finally:
        connection.close()


def repost(groups, post):
    for group_id in groups:
        if group_id not in deleted_groups:
            try:
                vk.messages.send(peer_id=group_id, attachment=f'wall-7679266_{post}', random_id=0)
            except vk_api.VkApiError:
                deleted_groups.add(group_id)


def info(moderators, maker):
    maker_json = vk.users.get(user_id=maker)
    information = f"Рассылкой занимается: {maker_json.get('first_name')} {maker_json.get('last_name')}"
    for moder in moderators:
        send_message(moder, information)


def post_sending():
    headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, "
                             "like Gecko) Chrome/81.0.4044.138 YaBrowser/20.6.2.195 Yowser/2.5 "
                             "Safari/537.36"}
    soup = BeautifulSoup(requests.get("https://vk.com/pb8mai", headers).content, "html.parser")
    convert = soup.findAll("a", {"class": "post__anchor anchor"})
    try:
        connection = pymysql.connect(host=my_host, user=my_user, password=my_pas, db=my_db,
                                     charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM last_post")
            last_post_id = [el.get("post_id") for el in cursor][0]
            posts = []
            for post_id in convert[:5]:
                number = str(post_id)[str(post_id).find("66_") + 3: str(post_id).find("></a") - 1]
                if int(number) > last_post_id:
                    posts.append(number)
                else:
                    break
            if len(posts) != 0:
                moderators = return_base("roles", "user_id")
                lock_users.update(moderators)
                for moder in moderators:
                    send_message(moder, f"Новых постов: {len(posts)}")
                groups = return_base("groups", "link")
                for post in posts[::-1]:
                    keyboard = VkKeyboard(one_time=False)
                    keyboard.add_button('Отправить', color=VkKeyboardColor.POSITIVE)
                    keyboard.add_button('Пропустить', color=VkKeyboardColor.NEGATIVE)

                    for moder in moderators:
                        vk.messages.send(peer_id=moder, attachment=f'wall-7679266_{post}', random_id=0,
                                         keyboard=keyboard.get_keyboard())

                    for input_event in long_poll.listen():
                        if input_event.type == VkBotEventType.MESSAGE_NEW and input_event.from_user:
                            if input_event.message.text.lower() == "отправить":
                                if len(moderators) != 1:
                                    moderators.pop(moderators.index(input_event.message.peer_id))
                                    Thread(target=info, args=(moderators, input_event.message.peer_id)).start()
                                    lock_users.difference_update(moderators)
                                    moderators = [input_event.message.peer_id]
                                Thread(target=repost, args=(groups, post)).start()
                                break
                            elif input_event.message.text.lower() == "пропустить":
                                if len(moderators) != 1:
                                    moderators.pop(moderators.index(input_event.message.peer_id))
                                    Thread(target=info, args=(moderators, input_event.message.peer_id)).start()
                                    lock_users.difference_update(moderators)
                                    moderators = [input_event.message.peer_id]
                                break
                            else:
                                send_message(input_event.message.peer_id, "Ожидается подтверждение публикации")
                cursor.execute(f"UPDATE last_post SET post_id={posts[0]}")
                for group_id in deleted_groups:
                    cursor.execute(f"DELETE FROM groups WHERE link={group_id}")
                connection.commit()
                vk.messages.send(peer_id=moderators[0], message="Все выбранные записи были разосланы", random_id=0,
                                 keyboard=VkKeyboard.get_empty_keyboard())
                lock_users.discard(moderators[0])
    finally:
        connection.close()


def add_access(user_id):
    try:
        connection = pymysql.connect(host=my_host, user=my_user, password=my_pas, db=my_db, charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        with connection.cursor() as cursor:
            cursor.execute(f"INSERT INTO roles (user_id) values({user_id})")
            connection.commit()
            send_message(user_id, "Доступ получен")
    finally:
        connection.close()


def delete_roles():
    out_list = "Выберите номер пункта для удаления из базы:\n"
    moderators = return_base("roles", "user_id")
    for pos, moderator in enumerate(vk.users.get(user_ids=moderators), 1):
        out_list += f"{pos}) {moderator.get('first_name')} {moderator.get('last_name')}\n"
    send_message(196595189, out_list)
    for input_event in long_poll.listen():
        if input_event.type == VkBotEventType.MESSAGE_NEW and input_event.message.peer_id == 196595189:
            if input_event.message.text == "/exit":
                lock_users.discard(196595189)
                break
            else:
                try:
                    number = int(input_event.message.text)
                except ValueError:
                    send_message(196595189, "Введите корректный номер пункта")
                else:
                    if 0 < number <= len(moderators):
                        try:
                            connection = pymysql.connect(host=my_host, user=my_user, password=my_pas, db=my_db,
                                                         charset='utf8mb4', cursorclass=pymysql.cursors.DictCursor)
                            with connection.cursor() as cursor:
                                cursor.execute(f"DELETE FROM roles WHERE user_id = {moderators[number - 1]}")
                                connection.commit()
                                send_message(196595189, "Пользователь удалён из базы")
                        finally:
                            connection.close()
                            lock_users.discard(196595189)
                            break
                    else:
                        send_message(196595189, "Введите корректный номер пункта")


class RepeatTimer(threading.Timer):
    def run(self):
        while not self.finished.wait(self.interval):
            self.function(*self.args, **self.kwargs)


RepeatTimer(300, post_sending).start()
for event in long_poll.listen():
    if event.type == VkBotEventType.MESSAGE_NEW:
        if event.from_chat:
            args = event.message.text.lower().split()
            if len(args) == 2 and args[0] == "/init":
                if len(args[1]) >= 5:
                    if event.message.peer_id in return_base("groups", "link"):
                        Thread(target=send_message, args=(event.message.peer_id, "Беседа уже находится в базе")).start()
                    else:
                        add_group(event.message.peer_id, args[1])
                else:
                    Thread(target=send_message, args=(event.message.peer_id, "Название должно включать не менее 5 "
                                                                             "символов")).start()
        elif event.from_user and event.message.peer_id not in lock_users:
            vk.messages.markAsRead(peer_id=event.message.peer_id)
            if event.message.peer_id in return_base("roles", "user_id"):
                if event.message.text.lower() == "/help":
                    command_list = "• /init $NAME - инициализировать беседу [/init М30-123Б-20]\n• /groups - вывести " \
                                   "все подключённые беседы "
                    Thread(target=send_message, args=(event.message.peer_id, command_list)).start()
                elif event.message.text.lower() == "/groups":
                    chats = "Беседы:".center(30, " ") + "\n"
                    for index, group in enumerate(return_base("groups", "title"), 1):
                        chats += f"{index}) {group}\n"
                    send_message(event.message.peer_id, chats)
                elif event.message.text.lower() == "/roles" and event.message.peer_id == 196595189:
                    lock_users.add(196595189)
                    Thread(target=delete_roles).start()
            elif event.message.text.lower() == "/boss 1519":
                add_access(event.message.peer_id)
                command_list = "• /help - список команд\n• /init $NAME - инициализировать беседу [/init " \
                               "М30-123Б-20]\n• /groups - вывести все подключённые беседы "
                Thread(target=send_message, args=(event.message.peer_id, command_list)).start()
