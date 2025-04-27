import time
import random


start_time = time.time()


LOG_FILE_NAME = 'big_log.txt'
LINE_COUNT = 1_000_000


with open(LOG_FILE_NAME, 'a', encoding='utf-8') as f:
    for _ in range(LINE_COUNT):
        avg = time.time() - random.random() * 1_000_000
        delta_fr = random.random() * 1000
        delta_to = random.random() * 1000

        fr = time.strftime("%Y-%m-%d %H:%M", time.localtime(avg - delta_fr))
        to = time.strftime("%Y-%m-%d %H:%M", time.localtime(avg + delta_to))

        f.write(f"FROM:{fr} TO:{to}\n")


end_time = time.time()

print(f"Время выполнения скрипта: {end_time - start_time:.4f} секунд")
