import time 


def send_to_queue(app_queue, applications):

    for app in applications:
        print(f"Sending {app['name']} to processing queue...")
        app_queue.put(app)
        time.sleep(0.2)
    print("All applications sent.\n")


def read_from_queue(result_queue):

    print("\n--- Evaluation Results --- ")
    while True:
       result = result_queue.get()

       if result == "STOP":
           break 
       
       print(result)