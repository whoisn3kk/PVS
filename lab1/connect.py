import hazelcast
import threading

import time

from traceback import format_exc

# default
def increment_counter(client, key, count):
    counter_map = client.get_map("counter-map").blocking()
    for _ in range(count):
        current_value = counter_map.get(key)
        counter_map.put(key, current_value + 1)

# pessimistic
def increment_counter_pessimistic(client, key, count):
    counter_map = client.get_map("counter-map").blocking()
    for i in range(count):
        counter_map.lock(key)
        try:
            current_value = counter_map.get(key)
            if current_value % 100 == 0:
                print(f"Completed {int(current_value / count * 100)}%")
            counter_map.put(key, current_value + 1)
        finally:
            counter_map.unlock(key)



# optimistic
def increment_counter_optimistic(client, key, count):
    counter_map = client.get_map("counter-map").blocking()
    for i in range(count):
        while True:
            old_value = counter_map.get(key)
            if old_value % 1000 == 0:
                print(f"Completed {int(old_value / count * 100)}%")
            new_value = (old_value or 0) + 1
            counter_map.replace_if_same(key, old_value, new_value)



# atomic
def increment_counter_atomic(client, key, count):
    atomic_counter = client._cp_subsystem().get_atomic_long("atomic-counter").blocking()
    for _ in range(count):
        atomic_counter.increment_and_get()
        if _ % 100 == 0:
            print(f"Completed {int(_ / count * 100)}%")

def worker(function, name):
    timer = time.time()
    # Создание Hazelcast клиента
    try:
        print("creating client")
        client = hazelcast.HazelcastClient(
            cluster_name="dev",
            cluster_members=[
                "localhost:5701",
                "localhost:5702",
                "localhost:5703"
            ]
        )

        # Создание и запуск потоков
        threads = []
        for _ in range(10):  # 10 потоков
            thread = threading.Thread(target=function, args=(client, "counter", 1000))
            threads.append(thread)
            thread.start()

        # Ожидание завершения всех потоков
        for thread in threads:
            thread.join()

        # Получение и печать итогового значения счетчика
        final_count = client.get_map("counter-map").blocking().get("counter")
        print(f"Final count is: {final_count}")
        print(f"Time spended on {pair[1]}: {time.time()-timer}\nDropping...\n\n")
        if name != "atomic": client.get_map("counter-map").blocking().put("counter", 0)
        # Закрытие клиента
        client.shutdown()
    except KeyboardInterrupt: 
        print(f"\n\nStopped by user, time spend {time.time()-timer}s")
        final_count = client.get_map("counter-map").blocking().get("counter")
        print(f"Final count is: {final_count}")
    except:
        print(format_exc())



# ,(increment_counter_pessimistic,'pessimistic')
FUNCTION_PAIRS = [(increment_counter,'default')]#,(increment_counter_pessimistic,'pessimistic'),(increment_counter_optimistic,'optimistic'),(increment_counter_atomic,'atomic')]


if __name__ == "__main__":
    for pair in FUNCTION_PAIRS:
        print(f"========= Starting test for {pair[1]} =========\n")
        worker(pair[0],pair[1])
        print(f"========= Test for {pair[1]} successfully ended =========\n")
        