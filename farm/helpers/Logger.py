import logging, sys, time

class Logger:
    def __init__(self):
        self.activatedLogger = False
        
    def animation(self, string=None):
        if string:
            sys.stdout.write(string)
            sys.stdout.flush()
        sys.stdout.write(".")
        sys.stdout.flush()
        time.sleep(0.8)
        sys.stdout.write(".")
        sys.stdout.flush()
        time.sleep(0.8)
        sys.stdout.write(".")
        sys.stdout.flush()
        time.sleep(1)
        print("\n")

    def activateLogger(self):
        self.activatedLogger = True
        return self


    def logprint(self, content, animated=False):
        if animated:
            self.animation(content)
        else:
            print(content)
        if self.activatedLogger:
            logging.info(content)

try:
    logging.basicConfig(format='%(message)s',filename='logs/datafarm.log', level=logging.INFO)
    globalLogger=Logger().activateLogger().logprint
except FileNotFoundError:
    print("No `logs` folder found. No logs will be stored...")
    globalLogger=Logger().logprint       
