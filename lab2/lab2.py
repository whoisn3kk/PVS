import time
from traceback import format_exc
import psycopg2
import threading
import os

from dotenv import load_dotenv
load_dotenv()
DBNAME = os.environ.get("DBNAME")
DB_USER = os.environ.get("DB_USER")
PASSWORD = os.environ.get("PASSWORD")
HOST = os.environ.get("HOST")

config = {
    "dbname":DBNAME, 
    "user":DB_USER, 
    "password":PASSWORD, 
    "host":HOST
}

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

def reset_counter():
    conn = psycopg2.connect(**config)
    cursor = conn.cursor()
    cursor.execute("UPDATE user_counter SET counter = 0, version = 0 WHERE USER_ID = 1")
    conn.commit()
    cursor.close()
    conn.close()
    # print(f"Counter has been reset to 0.")

def check_counter():
    conn = psycopg2.connect(**config)
    cursor = conn.cursor()
    cursor.execute("SELECT counter FROM user_counter WHERE user_id = 1")
    counter = cursor.fetchone()[0]
    conn.commit()
    cursor.close()
    conn.close()
    print(f"{bcolors.BOLD}[ {bcolors.WARNING}LOG{bcolors.ENDC}{bcolors.BOLD} ]{bcolors.ENDC} — value: {bcolors.BOLD}{bcolors.OKBLUE}{counter}{bcolors.ENDC}")
    return counter



### METHODS ###
# Lost-update
def lost_update():
    conn = psycopg2.connect(**config)
    cursor = conn.cursor()

    for _ in range(ITERATIONS):
        cursor.execute("SELECT counter FROM user_counter WHERE user_id = 1")
        counter = cursor.fetchone()[0]
        cursor.execute("UPDATE user_counter SET counter = %s WHERE user_id = 1", (counter + 1,))
        conn.commit()

    cursor.close()
    conn.close()


#In-place update
def in_place_update():
    conn = psycopg2.connect(**config)
    cursor = conn.cursor()

    for _ in range(ITERATIONS):
        cursor.execute("UPDATE user_counter SET counter = counter + 1 WHERE user_id = 1")
        conn.commit()

    cursor.close()
    conn.close()


#Row-level locking
def row_level_locking():
    conn = psycopg2.connect(**config)
    cursor = conn.cursor()

    for _ in range(ITERATIONS):
        cursor.execute("SELECT counter FROM user_counter WHERE user_id = 1 FOR UPDATE")
        counter = cursor.fetchone()[0]
        cursor.execute("UPDATE user_counter SET counter = %s WHERE user_id = 1", (counter + 1,))
        conn.commit()

    cursor.close()
    conn.close()


#Optimistic concurrency control
def optimistic_concurrency_control():
    conn = psycopg2.connect(**config)
    cursor = conn.cursor()

    for _ in range(10000):
        while True:
            cursor.execute("SELECT counter, version FROM user_counter WHERE user_id = 1")
            counter, version = cursor.fetchone()
            cursor.execute("UPDATE user_counter SET counter = %s, version = %s WHERE user_id = 1 AND version = %s", (counter + 1, version + 1, version))
            conn.commit()
            if cursor.rowcount > 0:
                break

    cursor.close()
    conn.close()




def main():
    THREADS = []

    def check_counter_thread(interval):
        while any(thread.is_alive() for thread in THREADS):
            conn = psycopg2.connect(**config)
            cursor = conn.cursor()
            cursor.execute("SELECT counter FROM user_counter WHERE user_id = 1")
            counter = cursor.fetchone()[0]
            conn.commit()
            cursor.close()
            conn.close()
            print(f"{bcolors.BOLD}[ {bcolors.WARNING}LOG{bcolors.ENDC}{bcolors.BOLD} ]{bcolors.ENDC} — value: {bcolors.BOLD}{bcolors.OKBLUE}{counter}{bcolors.ENDC}")
            time.sleep(interval)

    def worker(method):
        reset_counter()
        print(f"{bcolors.BOLD}[ {bcolors.WARNING}LOG{bcolors.ENDC}{bcolors.BOLD} ]{bcolors.ENDC} — counter reseted to {bcolors.BOLD}{bcolors.OKBLUE}0{bcolors.ENDC}\n")

        start_time = time.time()
        for _ in range(THREAD_COUNT):  
            if method == 'Lost-update':
                thread = threading.Thread(target=lost_update)
            elif method == 'In-place update':
                thread = threading.Thread(target=in_place_update)
            elif method == 'Row-level locking':
                thread = threading.Thread(target=row_level_locking)
            else:
                thread = threading.Thread(target=optimistic_concurrency_control)
            THREADS.append(thread)
            thread.start()

        monitor_thread = threading.Thread(target=check_counter_thread, args=(5,))
        monitor_thread.start()

        for thread in THREADS:
            thread.join()

        monitor_thread.join()

        final_count = check_counter()
        print(f"\n\n{bcolors.HEADER}Result for {bcolors.OKGREEN}{method}{bcolors.ENDC}")
        print(f"{bcolors.OKBLUE}Final counter value: {bcolors.WARNING}{final_count}{bcolors.ENDC}")
        print(f"{bcolors.BOLD}Time spend:  {bcolors.WARNING}{round(time.time()-start_time,2)}{bcolors.ENDC}{bcolors.BOLD}s{bcolors.ENDC}")
        print("————————————————————————————————————————————————\n\n\n")

    for method in NAMES:
        worker(method)

ITERATIONS = 10_000
THREAD_COUNT = 10
NAMES = ['Lost-update', 'In-place update', 'Row-level locking', 'Optimistic concurrency control']
if __name__ == "__main__":
    try:
        main()
    except:
        print(format_exc())