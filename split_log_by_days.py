import os
import fcntl
import logging
from datetime import datetime, timedelta
from typing import List, Tuple


LOG_FILE_NAME = 'big_log.txt'
BATCH_SIZE = 1000
OUTPUT_DIR = 'logs_by_day'
START_POSITION = 0
ERROR_LOG = 'error.log'

os.makedirs(OUTPUT_DIR, exist_ok=True)

logging.basicConfig(
    filename=ERROR_LOG,
    level=logging.ERROR,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)


def parse_record(line: str) -> Tuple[datetime, datetime]:
    """Разбирает строку лога и возвращает времена начала и конца."""
    fr_part, to_part = line.strip().split(' TO:')
    fr_time = datetime.strptime(fr_part.replace('FROM:', ''), "%Y-%m-%d %H:%M")
    to_time = datetime.strptime(to_part, "%Y-%m-%d %H:%M")
    return fr_time, to_time


def split_record_by_days(fr_time: datetime, to_time: datetime) -> List[Tuple[datetime, datetime]]:
    """Разбивает запись на части по дням, если она пересекает несколько дат."""
    results = []
    current = fr_time
    while current.date() < to_time.date():
        end_of_day = datetime.combine(current.date(), datetime.max.time()).replace(hour=23, minute=59, second=59)
        results.append((current, end_of_day))
        current = end_of_day + timedelta(seconds=1)
    results.append((current, to_time))
    return results


def write_record(day: str, fr_time: datetime, to_time: datetime, text: str) -> None:
    """Записывает запись в файл соответствующего дня с использованием блокировки."""
    filename = os.path.join(OUTPUT_DIR, f"{day}.log")
    with open(filename, 'a+') as f:
        try:
            fcntl.flock(f, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            logging.warning(f"Файл {filename} заблокирован, пропускаем.")
            return
        f.write(f"FROM:{fr_time.strftime('%Y-%m-%d %H:%M')} TO:{to_time.strftime('%Y-%m-%d %H:%M')}\n")
        fcntl.flock(f, fcntl.LOCK_UN)


def process_batch(batch: List[str]) -> None:
    """Обрабатывает пачку строк лога: парсит, разбивает по дням и сохраняет."""
    for line in batch:
        if not line.strip():
            continue
        try:
            fr_time, to_time = parse_record(line)
            splits = split_record_by_days(fr_time, to_time)
            for split_fr, split_to in splits:
                day = split_fr.strftime("%Y-%m-%d")
                write_record(day, split_fr, split_to, line)
        except Exception as e:
            logging.error(f"Ошибка парсинга строки: {line.strip()} - {e}")


def main() -> None:
    """Основной процесс: читает файл батчами и обрабатывает их."""
    batch = []
    position = START_POSITION

    try:
        with open(LOG_FILE_NAME, 'r') as log_file:
            log_file.seek(START_POSITION)

            while True:
                line = log_file.readline()
                if not line:
                    if batch:
                        process_batch(batch)
                    break

                batch.append(line)
                if len(batch) >= BATCH_SIZE:
                    process_batch(batch)
                    batch = []
                    position = log_file.tell()
                    print(f"Текущая позиция: {position}")

            position = log_file.tell()
    except KeyboardInterrupt:
        print("\nПрерывание по Ctrl+C, обрабатываю последний батч...")
        if batch:
            process_batch(batch)
        position = log_file.tell()
    except Exception as e:
        logging.exception(f"Ошибка в основном цикле: {e}")
    finally:
        print(f"Финальная позиция: {position}")


if __name__ == "__main__":
    main()
