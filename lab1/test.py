import hazelcast
import threading
import time

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

# Функція для інкрементування лічильника
def increment_counter(key, count, distributed_map):
    for _ in range(count):
        counter = distributed_map.get(key) or 0
        distributed_map.put(key, counter + 1)

def optimistic_increment(key, count, distributed_map):
    for _ in range(count):
        while True:
            # Получаем текущее значение и версию
            old_value = distributed_map.get(key)
            new_value = (old_value or 0) + 1

            # Пытаемся обновить значение, если оно не изменилось
            if distributed_map.replace_if_same(key, old_value, new_value):
                break

def pessimistic_increment(key, count, distributed_map):
    for _ in range(count):
        # Блокируем ключ
        distributed_map.lock(key)
        try:
            counter = distributed_map.get(key) or 0
            distributed_map.put(key, counter + 1)
        finally:
            # Важно разблокировать в блоке finally
            distributed_map.unlock(key)

def atomic_increment(key, count, client):
    atomic_long = client.cp_subsystem.get_atomic_long(key).blocking()
    for _ in range(count):
        atomic_long.increment_and_get()


def main():
    THREADS = []
    # Функція для моніторингу лічильника
    def monitor_counter(key, interval, client, monitor_type):
        if monitor_type == "map":
            distributed_map = client.get_map("counter_map").blocking()
            get_value = lambda: distributed_map.get(key) or 0
        elif monitor_type == "atomic":
            atomic_long = client.cp_subsystem.get_atomic_long(key).blocking()
            get_value = atomic_long.get
        while any(thread.is_alive() for thread in THREADS):
            current_count = get_value()
            print(f"{bcolors.BOLD}[ {bcolors.WARNING}LOG{bcolors.ENDC}{bcolors.BOLD} ]{bcolors.ENDC} — value: {bcolors.BOLD}{bcolors.OKBLUE}{current_count}{bcolors.ENDC}")
            time.sleep(interval)

    # Створення клієнта Hazelcast
    client = hazelcast.HazelcastClient(
        cluster_members=[
            "127.0.0.1:5701",
            "127.0.0.1:5702",
            "127.0.0.1:5703"
        ]
    )

    def worker(client, name):
        distributed_map = client.get_map("counter_map").blocking()
        distributed_map.put("counter_key", 0)
        atomic_long = client.cp_subsystem.get_atomic_long("counter_key").blocking()
        atomic_long.set(0)
        print(f"{bcolors.BOLD}[ {bcolors.WARNING}LOG{bcolors.ENDC}{bcolors.BOLD} ]{bcolors.ENDC} — counter reseted to {bcolors.BOLD}{bcolors.OKBLUE}0{bcolors.ENDC}\n")

        # Кількість ітерацій та потоків
        start_time = time.time()
        # Створення та запуск потоків для інкрементування
        for _ in range(THREAD_COUNT):
            if name == 'no lock':
                thread = threading.Thread(target=increment_counter, args=("counter_key", ITERATIONS, distributed_map))
            elif name == 'Pessimistic lock':
                thread = threading.Thread(target=pessimistic_increment, args=("counter_key", ITERATIONS, distributed_map))
            elif name == 'Optimistic lock':
                thread = threading.Thread(target=optimistic_increment, args=("counter_key", ITERATIONS, distributed_map))
            else:
                thread = threading.Thread(target=atomic_increment, args=("counter_key", ITERATIONS, client))
            thread.start()
            THREADS.append(thread)

        # Створення та запуск потоку для моніторингу
        monitor_type = "atomic" if name == 'IAtomicLong' else "map"
        monitor_thread = threading.Thread(target=monitor_counter, args=("counter_key", 5, client, monitor_type))
        monitor_thread.start()

        # Очікування завершення усіх потоків інкрементування
        for thread in THREADS:
            thread.join()

        # Очікування завершення потоку моніторингу
        monitor_thread.join()

        # Виведення кінцевого результату
        final_count = distributed_map.get("counter_key") or 0
        if name == "IAtomicLong":
            atomic_long = client.cp_subsystem.get_atomic_long("counter_key").blocking()
            final_count = atomic_long.get()
        print(f"\n\n{bcolors.HEADER}Result for {bcolors.OKGREEN}{name}{bcolors.ENDC}")
        print(f"{bcolors.OKBLUE}Final counter value: {bcolors.WARNING}{final_count}{bcolors.ENDC}")
        print(f"{bcolors.BOLD}Time spend:  {bcolors.WARNING}{round(time.time()-start_time,2)}{bcolors.ENDC}{bcolors.BOLD}s{bcolors.ENDC}")
        print("————————————————————————————————————————————————\n\n\n")

    for name in NAMES:
        worker(client, name)
    # Закриття клієнта Hazelcast
    client.shutdown()


# NAMES = ['IAtomicLong']
NAMES = ['no lock', 'Optimistic lock', 'Pessimistic lock', 'IAtomicLong']
ITERATIONS = 10_000
THREAD_COUNT = 10

if __name__ == "__main__":
    main()