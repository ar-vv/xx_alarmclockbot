import asyncio
import datetime as dt
import logging
import os
import re
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional

import aiosqlite
from aiogram import Bot, Dispatcher, F
from dotenv import load_dotenv

load_dotenv()
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (CallbackQuery, InlineKeyboardButton,
                           InlineKeyboardMarkup, KeyboardButton,
                           Message, ReplyKeyboardMarkup)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("bot.log", encoding="utf-8")
    ]
)
logger = logging.getLogger(__name__)

RUS_WEEKDAYS = [
    "понедельник",
    "вторник",
    "среда",
    "четверг",
    "пятница",
    "суббота",
    "воскресенье",
]


def format_alarm_datetime(dt_obj: dt.datetime) -> str:
    """Форматирует дату и время для будильника с днем недели"""
    today = dt.date.today()
    target_date = dt_obj.date()
    
    if target_date == today:
        day_name = "сегодня"
    elif target_date == today + dt.timedelta(days=1):
        day_name = "завтра"
    else:
        day_name = RUS_WEEKDAYS[target_date.weekday()]
    
    date_str = target_date.strftime("%d.%m")
    time_str = dt_obj.strftime("%H:%M")
    return f"<b>{day_name} ({date_str})</b> в <b>{time_str}</b>"


@dataclass
class Alarm:
    id: int
    user_id: int
    fire_at: dt.datetime
    note: Optional[str]


class AlarmStorage:
    def __init__(self, db_path: str = "alarms.db") -> None:
        self.db_path = db_path

    async def init(self) -> None:
        logger.info(f"Инициализация БД: {self.db_path}")
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute(
                """
                CREATE TABLE IF NOT EXISTS alarms (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER NOT NULL,
                    fire_at TEXT NOT NULL,
                    note TEXT
                )
                """
            )
            await db.commit()
        logger.info("БД инициализирована успешно")

    async def add_alarm(self, user_id: int, fire_at: dt.datetime, note: Optional[str]) -> int:
        logger.info(f"Добавление будильника: user_id={user_id}, fire_at={fire_at}, note={note}")
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "INSERT INTO alarms (user_id, fire_at, note) VALUES (?, ?, ?)",
                (user_id, fire_at.isoformat(), note),
            )
            await db.commit()
            alarm_id = cursor.lastrowid
            logger.info(f"Будильник добавлен с id={alarm_id}")
            return alarm_id

    async def list_alarms(self, user_id: int) -> List[Alarm]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute(
                "SELECT id, user_id, fire_at, note FROM alarms WHERE user_id = ? ORDER BY fire_at",
                (user_id,),
            )
            rows = await cursor.fetchall()
        return [self._row_to_alarm(row) for row in rows]

    async def delete_alarm(self, user_id: int, alarm_id: int) -> bool:
        logger.info(f"Удаление будильника: user_id={user_id}, alarm_id={alarm_id}")
        async with aiosqlite.connect(self.db_path) as db:
            cursor = await db.execute(
                "DELETE FROM alarms WHERE user_id = ? AND id = ?",
                (user_id, alarm_id),
            )
            await db.commit()
            deleted = cursor.rowcount > 0
            logger.info(f"Будильник {'удален' if deleted else 'не найден'}")
            return deleted

    async def delete_alarm_any_user(self, alarm_id: int) -> None:
        async with aiosqlite.connect(self.db_path) as db:
            await db.execute("DELETE FROM alarms WHERE id = ?", (alarm_id,))
            await db.commit()

    async def future_alarms(self) -> List[Alarm]:
        async with aiosqlite.connect(self.db_path) as db:
            db.row_factory = aiosqlite.Row
            cursor = await db.execute("SELECT id, user_id, fire_at, note FROM alarms")
            rows = await cursor.fetchall()
        alarms = [self._row_to_alarm(row) for row in rows]
        now = dt.datetime.now()
        return [alarm for alarm in alarms if alarm.fire_at > now]

    def _row_to_alarm(self, row: aiosqlite.Row) -> Alarm:
        return Alarm(
            id=row["id"],
            user_id=row["user_id"],
            fire_at=dt.datetime.fromisoformat(row["fire_at"]),
            note=row["note"],
        )


class AlarmScheduler:
    def __init__(self, storage: AlarmStorage, on_fire: Callable[[Alarm], asyncio.Future]) -> None:
        self.storage = storage
        self.on_fire = on_fire
        self.tasks: Dict[int, asyncio.Task] = {}

    async def load_existing(self) -> None:
        alarms = await self.storage.future_alarms()
        logger.info(f"Загрузка существующих будильников: найдено {len(alarms)}")
        for alarm in alarms:
            await self.schedule_alarm(alarm)
            logger.info(f"Запланирован будильник id={alarm.id} на {alarm.fire_at}")

    async def schedule_alarm(self, alarm: Alarm) -> None:
        logger.info(f"Планирование будильника id={alarm.id} на {alarm.fire_at}")
        self.cancel_alarm(alarm.id)
        task = asyncio.create_task(self._wait_and_fire(alarm))
        self.tasks[alarm.id] = task
        logger.debug(f"Задача создана для будильника id={alarm.id}")

    def cancel_alarm(self, alarm_id: int) -> None:
        task = self.tasks.pop(alarm_id, None)
        if task:
            task.cancel()

    async def _wait_and_fire(self, alarm: Alarm) -> None:
        delay = (alarm.fire_at - dt.datetime.now()).total_seconds()
        logger.info(f"Ожидание будильника id={alarm.id}, задержка={delay:.1f} сек")
        if delay > 0:
            try:
                await asyncio.sleep(delay)
                logger.info(f"Время будильника id={alarm.id} наступило!")
            except asyncio.CancelledError:
                logger.info(f"Будильник id={alarm.id} отменен")
                return
        await self.on_fire(alarm)
        self.tasks.pop(alarm.id, None)


class AlarmRuntime:
    def __init__(self, bot: Bot, alarm: Alarm, on_finish: Callable[[], None]):
        self.bot = bot
        self.alarm = alarm
        self.on_finish = on_finish
        self._stop_event = asyncio.Event()

    async def run(self) -> None:
        # Проверяем, является ли это ранним напоминанием
        if self.alarm.note and self.alarm.note.startswith("early_reminder:"):
            # Парсим данные раннего напоминания
            parts = self.alarm.note.split(":", 3)
            if len(parts) >= 4:
                minutes = parts[1]
                main_time = parts[2]
                user_text = parts[3] if len(parts) > 3 else "установлен будильник"
                time_labels = {
                    "5": "5 минут",
                    "15": "15 минут",
                    "60": "1 час",
                    "1440": "1 день"
                }
                time_label = time_labels.get(minutes, f"{minutes} минут")
                text = f"⏰ <b>Напоминаю, через {time_label}</b> в <b>{main_time}</b> у тебя {user_text}\n\nЧерез {time_label} напомню еще раз"
            else:
                text = f"🔔 <b>Напоминание!</b>\n\n{self.alarm.note}"
        elif self.alarm.note:
            text = f"🔔 <b>БУДИЛЬНИК!</b>\n\n💬 <b>Напоминание:</b> {self.alarm.note}"
        else:
            text = "🔔 <b>Дзынь-дзынь! Подъем-подъем!</b> ⏰"
        
        # Для ранних напоминаний используем другую кнопку
        if self.alarm.note and self.alarm.note.startswith("early_reminder:"):
            button = InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="🚫 Больше не напоминать", callback_data=f"ack:{self.alarm.id}")]]
            )
        else:
            button = InlineKeyboardMarkup(
                inline_keyboard=[[InlineKeyboardButton(text="✅ ок, увидел", callback_data=f"ack:{self.alarm.id}")]]
            )
        logger.info(f"Запуск процесса аларма для user_id={self.alarm.user_id}, alarm_id={self.alarm.id}")
        try:
            await self.bot.send_message(self.alarm.user_id, text, reply_markup=button)
            logger.debug(f"Первое сообщение отправлено user_id={self.alarm.user_id}")
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=60)
                logger.info(f"Аларм остановлен пользователем в течение минуты, user_id={self.alarm.user_id}")
                return
            except asyncio.TimeoutError:
                logger.info(f"Пользователь не ответил за минуту, начинаем спам, user_id={self.alarm.user_id}")

            for i in range(60):
                if self._stop_event.is_set():
                    logger.info(f"Аларм остановлен во время спама, user_id={self.alarm.user_id}")
                    return
                await self.bot.send_message(self.alarm.user_id, text, reply_markup=button)
                logger.debug(f"Отправка сообщения #{i+1} user_id={self.alarm.user_id}")
                try:
                    await asyncio.wait_for(self._stop_event.wait(), timeout=1)
                    logger.info(f"Аларм остановлен, user_id={self.alarm.user_id}")
                    return
                except asyncio.TimeoutError:
                    continue

            logger.warning(f"Не удалось достучаться до user_id={self.alarm.user_id}, отправка финального сообщения")
            await self.bot.send_message(
                self.alarm.user_id,
                "😴 Не смог до тебя достучаться, кажется ты все проспал...",
            )
        except Exception as e:
            logger.error(f"Ошибка в процессе аларма для user_id={self.alarm.user_id}: {e}", exc_info=True)
        finally:
            self.on_finish()
            logger.info(f"Процесс аларма завершен для user_id={self.alarm.user_id}")

    def stop(self) -> None:
        self._stop_event.set()


class AlarmRuntimeRegistry:
    def __init__(self) -> None:
        self.active: Dict[int, AlarmRuntime] = {}

    def start(self, runtime: AlarmRuntime) -> None:
        existing = self.active.get(runtime.alarm.user_id)
        if existing:
            existing.stop()
        self.active[runtime.alarm.user_id] = runtime
        asyncio.create_task(runtime.run())

    def stop_for_user(self, user_id: int) -> bool:
        runtime = self.active.pop(user_id, None)
        if runtime:
            runtime.stop()
            return True
        return False

    def stop_by_alarm(self, alarm_id: int) -> bool:
        for user_id, runtime in list(self.active.items()):
            if runtime.alarm.id == alarm_id:
                runtime.stop()
                self.active.pop(user_id, None)
                return True
        return False

    def is_active(self, user_id: int) -> bool:
        return user_id in self.active


class CreateAlarmStates(StatesGroup):
    choosing_day = State()
    choosing_hour = State()
    choosing_minute = State()
    entering_note = State()
    confirming_text_note = State()


class BotApp:
    def __init__(self, token: str, db_path: str = "alarms.db") -> None:
        self.bot = Bot(token=token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
        self.dp = Dispatcher()
        self.storage = AlarmStorage(db_path)
        self.runtime_registry = AlarmRuntimeRegistry()
        self.scheduler = AlarmScheduler(self.storage, self._on_alarm_fire)
        self.dp.message.register(
            self.intercept_alarm, lambda message, **_: self.runtime_registry.is_active(message.from_user.id)
        )
        self.dp.message.register(self.start_command, Command("start"))
        self.dp.message.register(self.stop_alarm, F.text.casefold() == "стоп")
        self.dp.message.register(self.show_menu, F.text.casefold() == "меню")
        self.dp.message.register(self.create_alarm_start, F.text.casefold() == "создать")
        self.dp.message.register(self.list_alarms, F.text.casefold() == "просмотреть")
        self.dp.message.register(self.delete_alarm_prompt, F.text.casefold() == "удалить")
        self.dp.callback_query.register(self.handle_ack, F.data.startswith("ack:"))

        self.dp.message.register(self.handle_day, CreateAlarmStates.choosing_day)
        self.dp.message.register(self.handle_hour, CreateAlarmStates.choosing_hour)
        self.dp.message.register(self.handle_minute, CreateAlarmStates.choosing_minute)
        self.dp.message.register(self.handle_note, CreateAlarmStates.entering_note)
        self.dp.message.register(self.handle_confirm_text_note, CreateAlarmStates.confirming_text_note)

        self.dp.message.register(self.handle_any_message)
        self.dp.callback_query.register(self.handle_confirm_create, F.data == "confirm_create")
        self.dp.callback_query.register(self.handle_cancel_create, F.data == "cancel_create")
        self.dp.callback_query.register(self.handle_early_reminder, F.data.startswith("early_reminder:"))
        self.dp.callback_query.register(self.handle_early_reminder_time, F.data.startswith("early_time:"))

    @property
    def menu_keyboard(self) -> ReplyKeyboardMarkup:
        return ReplyKeyboardMarkup(
            resize_keyboard=True,
            keyboard=[
                [KeyboardButton(text="Создать"), KeyboardButton(text="Просмотреть")],
                [KeyboardButton(text="Удалить"), KeyboardButton(text="Стоп")],
            ],
        )

    def day_keyboard(self) -> ReplyKeyboardMarkup:
        today = dt.date.today()
        buttons = [KeyboardButton(text="Сегодня"), KeyboardButton(text="Завтра")]
        for i in range(2, 7):
            day = today + dt.timedelta(days=i)
            text = f"{RUS_WEEKDAYS[day.weekday()].capitalize()} ({day.day}.{day.month})"
            buttons.append(KeyboardButton(text=text))
        rows = [[btn] for btn in buttons]
        return ReplyKeyboardMarkup(resize_keyboard=True, keyboard=rows)

    def hour_keyboard(self) -> ReplyKeyboardMarkup:
        buttons = [KeyboardButton(text=str(h)) for h in range(24)]
        rows = [buttons[i : i + 6] for i in range(0, len(buttons), 6)]
        return ReplyKeyboardMarkup(resize_keyboard=True, keyboard=rows)

    def minute_keyboard(self) -> ReplyKeyboardMarkup:
        buttons = [KeyboardButton(text=f"{m} минут") for m in range(0, 60, 10)]
        rows = [buttons[i : i + 3] for i in range(0, len(buttons), 3)]
        return ReplyKeyboardMarkup(resize_keyboard=True, keyboard=rows)

    def note_keyboard(self) -> ReplyKeyboardMarkup:
        return ReplyKeyboardMarkup(resize_keyboard=True, keyboard=[[KeyboardButton(text="Пропустить")]])

    async def start(self) -> None:
        logger.info("Запуск бота...")
        await self.storage.init()
        logger.info("Загрузка существующих будильников...")
        await self.scheduler.load_existing()
        logger.info("Бот запущен и готов к работе!")
        await self.dp.start_polling(self.bot)

    async def start_command(self, message: Message, state: FSMContext) -> None:
        logger.info(f"Команда /start от user_id={message.from_user.id}")
        await state.clear()
        await message.answer(
            "👋 <b>Привет! Я будильник-бот</b> ⏰\n\n"
            "• <b>Создать</b> — новый будильник\n"
            "• <b>Просмотреть</b> — список будильников\n"
            "• <b>Удалить</b> — убрать любой будильник\n"
            "• <b>Стоп</b> — экстренно глушит звонок 🔕",
            reply_markup=self.menu_keyboard,
        )

    async def show_menu(self, message: Message, state: FSMContext) -> None:
        await state.clear()
        await message.answer("Готово! Выбирай действие 👇", reply_markup=self.menu_keyboard)

    async def create_alarm_start(self, message: Message, state: FSMContext) -> None:
        logger.info(f"Начало создания будильника для user_id={message.from_user.id}")
        await state.clear()  # Очищаем состояние, чтобы начать с чистого листа
        await state.set_state(CreateAlarmStates.choosing_day)
        await message.answer(
            "📅 <b>Выбери день</b>\n\nНажми кнопку или пришли дату формата <b>ДД.ММ</b> (любой разделитель).",
            reply_markup=self.day_keyboard()
        )

    async def handle_day(self, message: Message, state: FSMContext) -> None:
        logger.debug(f"Обработка дня от user_id={message.from_user.id}, текст: {message.text}")
        parsed_date = self.parse_date(message.text)
        if not parsed_date:
            logger.warning(f"Не удалось распарсить дату: {message.text}")
            await message.answer(
                "❌ Не понял дату. Напиши <b>день и месяц</b> цифрами (например <b>1.9</b>) или нажми кнопку.",
                reply_markup=self.day_keyboard()
            )
            return
        logger.info(f"Выбран день: {parsed_date} для user_id={message.from_user.id}")
        await state.update_data(day=parsed_date.isoformat())
        await state.set_state(CreateAlarmStates.choosing_hour)
        await message.answer(
            "🕐 <b>Выбери часы</b> (0-23)\n\nНажми кнопку или пришли время текстом вида <b>07:30</b> — тогда сразу пойму и минуты.",
            reply_markup=self.hour_keyboard(),
        )

    async def handle_hour(self, message: Message, state: FSMContext) -> None:
        numbers = self.extract_numbers(message.text)
        if len(numbers) >= 2:
            hour, minute = numbers[0], numbers[1]
            if not self.valid_hour_minute(hour, minute):
                await message.answer("❌ Часы/минуты вне диапазона. Жду <b>часы 0-23</b> и <b>минуты 0-59</b>.", reply_markup=self.hour_keyboard())
                return
            await state.update_data(hour=hour, minute=minute)
            # Всегда предлагаем ввести текст напоминания, если его еще нет
            await self.ask_note(message, state)
            return

        if len(numbers) == 0:
            await message.answer("❌ Нужны <b>часы</b> цифрами 0-23. Попробуй снова.", reply_markup=self.hour_keyboard())
            return

        hour = numbers[0]
        if not 0 <= hour <= 23:
            await message.answer("❌ Часы бывают от <b>0 до 23</b>. Введи корректное число.", reply_markup=self.hour_keyboard())
            return

        await state.update_data(hour=hour)
        await state.set_state(CreateAlarmStates.choosing_minute)
        await message.answer(
            "⏱ <b>Теперь минуты</b>\n\nВыбери: 0, 10, 20, 30, 40 или 50. Можно просто прислать число.",
            reply_markup=self.minute_keyboard()
        )

    async def handle_minute(self, message: Message, state: FSMContext) -> None:
        numbers = self.extract_numbers(message.text)
        if not numbers:
            await message.answer("❌ Минуты нужны числом <b>0-59</b>. Попробуем еще раз.", reply_markup=self.minute_keyboard())
            return
        minute = numbers[0]
        if not 0 <= minute <= 59:
            await message.answer("❌ Минуты от <b>0 до 59</b>, не больше.", reply_markup=self.minute_keyboard())
            return
        await state.update_data(minute=minute)
        await self.ask_note(message, state)

    async def ask_note(self, message: Message, state: FSMContext) -> None:
        data = await state.get_data()
        # Если текст уже есть (например, из предложения создать с текстом), пропускаем запрос
        if data.get("note"):
            await self.finalize_alarm(message, state)
            return
        await state.set_state(CreateAlarmStates.entering_note)
        await message.answer(
            "💬 <b>Добавь текст напоминания</b> (опционально)\n\nИли нажми <b>Пропустить</b>, если не нужен.",
            reply_markup=self.note_keyboard()
        )

    async def handle_note(self, message: Message, state: FSMContext) -> None:
        note = None if message.text.strip().lower() == "пропустить" else message.text.strip()
        await state.update_data(note=note)
        await self.finalize_alarm(message, state)

    async def finalize_alarm(self, message: Message, state: FSMContext) -> None:
        data = await state.get_data()
        logger.debug(f"Финализация будильника, данные: {data}")
        
        # Проверяем, что все необходимые данные есть
        if "minute" not in data or "day" not in data:
            logger.warning(f"Недостаточно данных для создания будильника: {data}")
            await message.answer("❌ Что-то пошло не так. Начнем заново?", reply_markup=self.menu_keyboard)
            await state.clear()
            return

        target_day = dt.date.fromisoformat(data["day"])
        hour = data.get("hour", 0)
        minute = data.get("minute", 0)
        fire_at = dt.datetime.combine(target_day, dt.time(hour=hour, minute=minute))
        if fire_at <= dt.datetime.now():
            logger.warning(f"Попытка создать будильник в прошлом: {fire_at}")
            await message.answer("⏰ Это время уже было. Давай выберем день заново?", reply_markup=self.day_keyboard())
            await state.set_state(CreateAlarmStates.choosing_day)
            return

        note = data.get("note")  # Может быть None, если пользователь пропустил
        alarm_id = await self.storage.add_alarm(message.from_user.id, fire_at, note)
        alarm = Alarm(id=alarm_id, user_id=message.from_user.id, fire_at=fire_at, note=note)
        await self.scheduler.schedule_alarm(alarm)
        await state.clear()

        formatted_datetime = format_alarm_datetime(fire_at)
        note_text = f"\n\n💬 <b>Напоминание:</b> {note}" if note else ""
        logger.info(f"Будильник создан: id={alarm_id}, user_id={message.from_user.id}, время={fire_at}")
        
        # Создаем клавиатуру с кнопкой "Напомнить заранее"
        early_reminder_markup = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="⏰ Напомнить заранее", callback_data=f"early_reminder:{alarm_id}")]
            ]
        )
        
        await message.answer(
            f"✅ <b>Будильник запланирован!</b>\n\n⏰ {formatted_datetime}{note_text}\n\n"
            f"Я обязательно напомню! 🔔\n\n"
            f"💡 <i>Могу также напомнить заранее до установленного времени</i>",
            reply_markup=early_reminder_markup
        )
        
        # Отправляем меню
        await message.answer("🙏 Спасибо за использование!", reply_markup=self.menu_keyboard)

    async def list_alarms(self, message: Message, state: FSMContext) -> None:
        alarms = await self.storage.list_alarms(message.from_user.id)
        if not alarms:
            await message.answer("📭 У тебя пока нет будильников.", reply_markup=self.menu_keyboard)
            return
        lines = []
        for alarm in alarms:
            formatted = format_alarm_datetime(alarm.fire_at)
            text = f"🔔 {formatted}"
            if alarm.note:
                text += f"\n   💬 {alarm.note}"
            lines.append(text)
        await message.answer("📋 <b>Твои будильники:</b>\n\n" + "\n\n".join(lines), reply_markup=self.menu_keyboard)

    async def delete_alarm_prompt(self, message: Message, state: FSMContext) -> None:
        alarms = await self.storage.list_alarms(message.from_user.id)
        if not alarms:
            await message.answer("🗑 Удалять нечего, будильников нет.", reply_markup=self.menu_keyboard)
            return
        buttons = []
        for alarm in alarms:
            today = dt.date.today()
            target_date = alarm.fire_at.date()
            if target_date == today:
                day_name = "сегодня"
            elif target_date == today + dt.timedelta(days=1):
                day_name = "завтра"
            else:
                day_name = RUS_WEEKDAYS[target_date.weekday()]
            date_str = target_date.strftime("%d.%m")
            time_str = alarm.fire_at.strftime("%H:%M")
            note_text = f" — {alarm.note[:15]}..." if alarm.note and len(alarm.note) > 15 else (f" — {alarm.note}" if alarm.note else "")
            button_text = f"🗑 {day_name} ({date_str}) {time_str}{note_text}"
            buttons.append([InlineKeyboardButton(text=button_text, callback_data=f"del:{alarm.id}")])
        markup = InlineKeyboardMarkup(inline_keyboard=buttons)
        await message.answer("🗑 <b>Выбери будильник для удаления:</b>", reply_markup=markup)

    async def handle_ack(self, callback: CallbackQuery) -> None:
        alarm_id = int(callback.data.split(":", 1)[1])
        stopped = self.runtime_registry.stop_by_alarm(alarm_id)
        if stopped:
            await callback.message.answer("✅ <b>Будильник выключен</b>, рад был помочь! 😊")
            await callback.answer()
            return
        await callback.answer("Будильник уже тихонечко ушел")

    async def stop_alarm(self, message: Message, state: FSMContext) -> None:
        stopped = self.runtime_registry.stop_for_user(message.from_user.id)
        if stopped:
            await message.answer("✅ <b>Будильник выключен</b>, рад был помочь! 😊", reply_markup=self.menu_keyboard)
        else:
            await message.answer("ℹ️ Эта кнопка для <b>экстренной остановки звонка</b>. Пока звонков нет. 🔕", reply_markup=self.menu_keyboard)

    async def intercept_alarm(self, message: Message, state: FSMContext) -> None:
        if self.runtime_registry.stop_for_user(message.from_user.id):
            await message.answer("✅ <b>Будильник выключен</b>, рад был помочь! 😊", reply_markup=self.menu_keyboard)

    async def handle_any_message(self, message: Message, state: FSMContext) -> None:
        logger.debug(f"Получено сообщение от user_id={message.from_user.id}, текст: {message.text}")
        if self.runtime_registry.stop_for_user(message.from_user.id):
            logger.info(f"Аларм остановлен сообщением от user_id={message.from_user.id}")
            await message.answer("✅ <b>Будильник выключен</b>, рад был помочь! 😊", reply_markup=self.menu_keyboard)
            return
        
        # Если есть текст и нет активного процесса аларма - предложить создать будильник
        if message.text and message.text.strip():
            current_state = await state.get_state()
            # Не предлагать, если уже в процессе создания будильника
            if current_state not in [CreateAlarmStates.choosing_day, CreateAlarmStates.choosing_hour, 
                                     CreateAlarmStates.choosing_minute, CreateAlarmStates.entering_note,
                                     CreateAlarmStates.confirming_text_note]:
                await self.offer_create_with_text(message, state, message.text.strip())
                return
        
        await message.answer(
            "👋 Выбирай действие:\n"
            "• <b>Создать</b> — новый будильник\n"
            "• <b>Просмотреть</b> — список\n"
            "• <b>Удалить</b> — убрать\n"
            "• <b>Стоп</b> — глушит звонок 🔕",
            reply_markup=self.menu_keyboard,
        )
    
    async def offer_create_with_text(self, message: Message, state: FSMContext, text: str) -> None:
        """Предлагает создать будильник с указанным текстом напоминания"""
        await state.set_state(CreateAlarmStates.confirming_text_note)
        await state.update_data(note=text)
        markup = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="✅ Создать", callback_data="confirm_create")],
                [InlineKeyboardButton(text="❌ Отменить", callback_data="cancel_create")]
            ]
        )
        await message.answer(
            f"💬 <b>Создать напоминание?</b>\n\n"
            f"📝 <b>Текст:</b> {text}\n\n"
            f"Нажми <b>Создать</b>, чтобы установить будильник с этим напоминанием.",
            reply_markup=markup
        )
    
    async def handle_confirm_text_note(self, message: Message, state: FSMContext) -> None:
        """Обработка текста при подтверждении создания с текстом"""
        # Если пользователь прислал текст в состоянии подтверждения, обновляем текст
        if message.text and message.text.strip():
            await state.update_data(note=message.text.strip())
            await self.offer_create_with_text(message, state, message.text.strip())
    
    async def handle_confirm_create(self, callback: CallbackQuery, state: FSMContext) -> None:
        """Обработка кнопки 'Создать' - начинаем процесс создания будильника"""
        data = await state.get_data()
        note = data.get("note")
        await state.update_data(note=note)  # Сохраняем текст напоминания
        await state.set_state(CreateAlarmStates.choosing_day)
        await callback.message.edit_text(
            f"✅ Отлично! Создаем будильник с напоминанием:\n\n"
            f"💬 <b>{note}</b>\n\n"
            f"📅 <b>Выбери день</b>\n\n"
            f"Нажми кнопку или пришли дату формата <b>ДД.ММ</b> (любой разделитель).",
            reply_markup=None
        )
        await callback.message.answer(
            "📅 <b>Выбери день</b>\n\nНажми кнопку или пришли дату формата <b>ДД.ММ</b> (любой разделитель).",
            reply_markup=self.day_keyboard()
        )
        await callback.answer()
    
    async def handle_cancel_create(self, callback: CallbackQuery, state: FSMContext) -> None:
        """Обработка кнопки 'Отменить'"""
        await state.clear()
        await callback.message.edit_text("❌ Создание будильника отменено")
        await callback.message.answer(
            "👋 Нажимай кнопки меню:\n"
            "• <b>Создать</b> — новый будильник\n"
            "• <b>Просмотреть</b> — список\n"
            "• <b>Удалить</b> — убрать\n"
            "• <b>Стоп</b> — глушит звонок 🔕",
            reply_markup=self.menu_keyboard,
        )
        await callback.answer()
    
    async def handle_early_reminder(self, callback: CallbackQuery) -> None:
        """Обработка кнопки 'Напомнить заранее'"""
        alarm_id = int(callback.data.split(":", 1)[1])
        # Получаем информацию о будильнике
        alarms = await self.storage.list_alarms(callback.from_user.id)
        alarm = next((a for a in alarms if a.id == alarm_id), None)
        
        if not alarm:
            await callback.answer("❌ Будильник не найден", show_alert=True)
            return
        
        # Создаем клавиатуру с вариантами времени
        markup = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text="⏰ За 5 минут", callback_data=f"early_time:{alarm_id}:5")],
                [InlineKeyboardButton(text="⏰ За 15 минут", callback_data=f"early_time:{alarm_id}:15")],
                [InlineKeyboardButton(text="⏰ За час", callback_data=f"early_time:{alarm_id}:60")],
                [InlineKeyboardButton(text="⏰ За день", callback_data=f"early_time:{alarm_id}:1440")]
            ]
        )
        
        formatted = format_alarm_datetime(alarm.fire_at)
        await callback.message.edit_text(
            f"⏰ <b>Напомнить заранее?</b>\n\n"
            f"Основной будильник: {formatted}\n\n"
            f"Выбери, за сколько времени напомнить:",
            reply_markup=markup
        )
        await callback.answer()
    
    async def handle_early_reminder_time(self, callback: CallbackQuery) -> None:
        """Обработка выбора времени для напоминания заранее"""
        parts = callback.data.split(":")
        alarm_id = int(parts[1])
        minutes = int(parts[2])
        
        # Получаем информацию о будильнике
        alarms = await self.storage.list_alarms(callback.from_user.id)
        alarm = next((a for a in alarms if a.id == alarm_id), None)
        
        if not alarm:
            await callback.answer("❌ Будильник не найден", show_alert=True)
            return
        
        # Вычисляем время для раннего напоминания
        early_time = alarm.fire_at - dt.timedelta(minutes=minutes)
        
        # Проверяем, что время не в прошлом
        if early_time <= dt.datetime.now():
            await callback.answer("❌ Нельзя установить напоминание в прошлом", show_alert=True)
            return
        
        # Создаем текст для раннего напоминания
        time_labels = {
            5: "5 минут",
            15: "15 минут",
            60: "1 час",
            1440: "1 день"
        }
        early_note = f"⏰ Напоминание: через {time_labels[minutes]} — {alarm.note if alarm.note else 'Будильник'}"
        
        # Создаем дополнительный будильник
        early_alarm_id = await self.storage.add_alarm(callback.from_user.id, early_time, early_note)
        early_alarm = Alarm(id=early_alarm_id, user_id=callback.from_user.id, fire_at=early_time, note=early_note)
        await self.scheduler.schedule_alarm(early_alarm)
        
        formatted_early = format_alarm_datetime(early_time)
        formatted_main = format_alarm_datetime(alarm.fire_at)
        
        logger.info(f"Создано раннее напоминание: id={early_alarm_id}, основной id={alarm_id}, за {minutes} минут")
        
        await callback.message.edit_text(
            f"✅ <b>Раннее напоминание установлено!</b>\n\n"
            f"⏰ <b>Напомню:</b> {formatted_early}\n"
            f"🔔 <b>Основной будильник:</b> {formatted_main}\n\n"
            f"Напомню за <b>{time_labels[minutes]}</b>",
            reply_markup=None
        )
        await callback.answer("✅ Раннее напоминание установлено!")

    async def delete_alarm(self, callback: CallbackQuery) -> None:
        alarm_id = int(callback.data.split(":", 1)[1])
        deleted = await self.storage.delete_alarm(callback.from_user.id, alarm_id)
        self.scheduler.cancel_alarm(alarm_id)
        if deleted:
            await callback.message.answer("✅ <b>Будильник удален</b>", reply_markup=self.menu_keyboard)
        else:
            await callback.message.answer("❌ Не нашел такой будильник.", reply_markup=self.menu_keyboard)
        await callback.answer()

    async def _on_alarm_fire(self, alarm: Alarm) -> None:
        logger.info(f"🔥 СРАБАТЫВАНИЕ БУДИЛЬНИКА id={alarm.id} для user_id={alarm.user_id}!")
        await self.storage.delete_alarm_any_user(alarm.id)
        runtime = AlarmRuntime(self.bot, alarm, lambda: self.runtime_registry.active.pop(alarm.user_id, None))
        self.runtime_registry.start(runtime)

    def parse_date(self, text: str) -> Optional[dt.date]:
        normalized = text.strip().lower()
        today = dt.date.today()
        if normalized.startswith("сегодня"):
            return today
        if normalized.startswith("завтра"):
            return today + dt.timedelta(days=1)

        pattern_days = {}
        for i in range(2, 7):
            day = today + dt.timedelta(days=i)
            label = f"{RUS_WEEKDAYS[day.weekday()]} ({day.day}.{day.month})"
            pattern_days[label] = day
        for label, day in pattern_days.items():
            if normalized.startswith(label.split(" ")[0]):
                return day

        numbers = self.extract_numbers(text)
        if len(numbers) < 2:
            return None
        day_num, month_num = numbers[0], numbers[1]
        year = today.year
        try:
            candidate = dt.date(year, month_num, day_num)
        except ValueError:
            return None
        if candidate < today:
            try:
                candidate = dt.date(year + 1, month_num, day_num)
            except ValueError:
                return None
        return candidate

    def extract_numbers(self, text: str) -> List[int]:
        return [int(x) for x in re.findall(r"\d+", text)]

    def valid_hour_minute(self, hour: int, minute: int) -> bool:
        return 0 <= hour <= 23 and 0 <= minute <= 59


def register_callbacks(app: BotApp) -> None:
    app.dp.callback_query.register(app.delete_alarm, F.data.startswith("del:"))


def main() -> None:
    logger.info("=" * 50)
    logger.info("Запуск Telegram Alarm Clock Bot")
    logger.info("=" * 50)
    token = os.environ.get("TELEGRAM_BOT_TOKEN")
    if not token:
        logger.error("TELEGRAM_BOT_TOKEN не установлен!")
        raise RuntimeError("TELEGRAM_BOT_TOKEN not set")
    logger.info(f"Токен получен (длина: {len(token)} символов)")
    app = BotApp(token)
    register_callbacks(app)
    try:
        asyncio.run(app.start())
    except KeyboardInterrupt:
        logger.info("Получен сигнал остановки, завершение работы...")
    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    main()
