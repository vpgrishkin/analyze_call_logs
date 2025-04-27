import os
import re
import sys
import time
import fcntl
import heapq
import multiprocessing
from datetime import datetime
from functools import partial
from typing import List, Tuple, cast, TYPE_CHECKING

import questionary
from rich.console import Console
from rich.prompt import IntPrompt
from multiprocessing import Manager

if TYPE_CHECKING:
    from multiprocessing.managers import ValueProxy

LOGS_DIR: str = './fixtures/logs_by_day'
FILE_PATTERN: re.Pattern[str] = re.compile(r'\d{4}-\d{2}-\d{2}\.log$')
MAX_EXAMPLES: int = 5

console: Console = Console()

manager: Manager = Manager()
active_tasks_counter = cast("ValueProxy[int]", manager.Value('i', 0))


def list_log_files(path: str) -> List[str]:
    """Возвращает список файлов в указанной директории."""
    files = []
    for fname in os.listdir(path):
        full_path = os.path.join(path, fname)
        if os.path.isfile(full_path):
            files.append(fname)
    return files


def select_files(files: List[str]) -> List[str]:
    """Позволяет выбрать файлы для обработки."""
    if not files:
        return []

    choices = [{"name": "[Выбрать все файлы]", "checked": False}] + [{"name": f, "checked": False} for f in files]

    selected = questionary.checkbox(
        "Выберите файлы для обработки (Пробел - выделить, Enter - подтвердить):",
        choices=choices
    ).ask()

    if not selected:
        return []

    if "[Выбрать все файлы]" in selected:
        return files
    else:
        return selected


def read_log_file(filepath: str) -> List[Tuple[datetime, datetime]]:
    """Читает лог-файл и возвращает список пар (время начала, время окончания)."""
    calls = []
    with open(filepath, 'r') as f:
        try:
            fcntl.flock(f, fcntl.LOCK_SH | fcntl.LOCK_NB)
        except BlockingIOError:
            console.print(f"[yellow]Файл {filepath} заблокирован, пропускаем.[/yellow]")
            return []
        for line in f:
            match = re.match(r"FROM:(.*?) TO:(.*)", line.strip())
            if match:
                from_time = datetime.strptime(match.group(1), "%Y-%m-%d %H:%M")
                to_time = datetime.strptime(match.group(2), "%Y-%m-%d %H:%M")
                calls.append((from_time, to_time))
        fcntl.flock(f, fcntl.LOCK_UN)
    return calls


def calculate_min_operators(calls: List[Tuple[datetime, datetime]]) -> Tuple[int, List[Tuple[datetime, datetime]]]:
    """Рассчитывает минимальное количество операторов, необходимых для обработки всех звонков."""
    events = sorted(calls, key=lambda x: x[0])
    heap: List[Tuple[datetime, datetime]] = []
    max_operators: int = 0
    peak_calls: List[Tuple[datetime, datetime]] = []

    for fr_time, to_time in events:
        while heap and heap[0][1] <= fr_time:
            heapq.heappop(heap)
        heapq.heappush(heap, (fr_time, to_time))
        if len(heap) > max_operators:
            max_operators = len(heap)
            peak_calls = heap.copy()

    return max_operators, peak_calls


def process_file(log_dir: str, filename: str) -> Tuple[str, int, List[Tuple[datetime, datetime]]]:
    """Обрабатывает отдельный файл логов и возвращает статистику по операторам."""
    filepath = os.path.join(log_dir, filename)
    calls = read_log_file(filepath)
    if not calls:
        return filename, 0, []
    max_operators, peak = calculate_min_operators(calls)
    return filename, max_operators, peak


def worker_wrapper(log_dir: str, filename: str, active_tasks: "ValueProxy[int]") -> Tuple[
        str, int, List[Tuple[datetime, datetime]]]:
    """Обертка для увеличения/уменьшения счётчика задач."""
    active_tasks.value += 1
    try:
        result = process_file(log_dir, filename)
    finally:
        active_tasks.value -= 1
    return result


def main() -> None:
    console.clear()
    console.rule("[bold green]Анализ файлов логов[/bold green]")

    files = list_log_files(LOGS_DIR)
    valid_files = [fname for fname in files if FILE_PATTERN.match(fname)]

    if not valid_files:
        console.print("[bold red]Нет файлов для обработки![/bold red]")
        return

    selected_files = select_files(valid_files)

    if not selected_files:
        console.print("[bold yellow]Файлы не выбраны. Завершение.[/bold yellow]")
        return

    console.clear()
    console.print(f"[green]Выбрано файлов для обработки: {len(selected_files)}[/green]")

    workers = IntPrompt.ask("Введите количество процессов", default=4)

    pool = multiprocessing.Pool(processes=workers)

    worker = partial(worker_wrapper, LOGS_DIR, active_tasks=active_tasks_counter)
    results = pool.map_async(worker, selected_files)

    try:
        while not results.ready():
            console.print(f"[cyan]Обрабатывается файлов: {active_tasks_counter.value}[/cyan]", end="\r")
            time.sleep(0.5)

        final_results = results.get()

        pool.close()
        pool.join()

    except KeyboardInterrupt:
        console.print("\n[red]Остановка обработки по Ctrl+C...[/red]")
        pool.terminate()
        pool.join()
        sys.exit(0)

    console.rule("[bold cyan]Результаты[/bold cyan]")
    for filename, max_ops, peak in final_results:
        if max_ops == 0:
            console.print(f"[yellow]{filename}: Нет звонков[/yellow]")
            continue

        console.print(f"[bold]{filename}[/bold]: Минимальное количество операторов: [green]{max_ops}[/green]")
        console.print("[italic]Примеры пересечений:[/italic]")
        for from_time, to_time in sorted(peak)[:MAX_EXAMPLES]:
            console.print(f"  FROM:{from_time.strftime('%Y-%m-%d %H:%M')} TO:{to_time.strftime('%Y-%m-%d %H:%M')}")

        if len(peak) > MAX_EXAMPLES:
            console.print("  ...")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        console.print("\n[red]Остановка обработки по Ctrl+C...[/red]")
        sys.exit(0)
