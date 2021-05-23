import logging, sys, time
try:
    logging.basicConfig(format='%(message)s',filename='logs/datafarm.log', level=logging.INFO)
except FileNotFoundError:
    logging.basicConfig(format='%(message)s',filename='../../logs/datafarm.log', level=logging.INFO)


class Logger:
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
            global activatedLogger
            activatedLogger = True
            return self


        def logprint(self, content, animated=False):
            if animated:
                self.animation(content)
            else:
                print(content)
            if activatedLogger:
                logging.info(content)

                
globalLogger=Logger().activateLogger().logprint