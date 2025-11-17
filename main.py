import multiprocessing
import threading 
import time
from evaluator import evaluate_application 
from communicator import send_to_queue, read_from_queue


def main():
    print("Starting HireFlow Checkpoint...\n")

    #Test applications 

    applications = [
    {"name": "Alex", "experience": 7},
    {"name": "Robby", "experience": 6},
    {"name": "James", "experience": 2},
    {"name": "Tyler", "experience": 2},
]   
    
    app_queue = multiprocessing.Queue()
    result_queue = multiprocessing.Queue()

    start_time = time.time() 

    sender_thread = threading.Thread(target=send_to_queue, args=(app_queue, applications))
    sender_thread.start()
    sender_thread.join()

    processes = []
    for _ in range(2):
        p = multiprocessing.Process(target=evaluate_application, args=(app_queue, result_queue))
        p.start()
        processes.append(p)
    

    printer_thread = threading.Thread(target=read_from_queue, args=(result_queue,))
    printer_thread.start()


    for p in processes:
        p.join()

    result_queue.put("STOP")

    printer_thread.join()

    end_time = time.time()
    print(f"\n All application processed in {round(end_time - start_time, 2)} seconds.")

if __name__ == "__main__":
    main()


