import time
import random


def evaluate_application(app_queue, result_queue):

    while not app_queue.empty():
        try:
            app = app_queue.get_nowait()
        except:
            break

        print(f"Evaluating {app['name']}...")

        time.sleep(random.uniform(0.5, 1.5))

        if app["experience"] >= 3:
            result = f"{app['name']} -> Qualified "
        else:
            result = f"{app['name']} -> Not Qualified "

        result_queue.put(result)


def evaluate_single(app):

    print(f"Evaluating {app['name']}...")
    time.sleep(random.uniform(0.5, 1.5))

    if app["experience"] >= 3:
        return f"{app['name']} -> Qualified "
    else:
        return f"{app['name']} -> Not Qualified "
