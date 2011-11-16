import condor

def square(x):
    return x**2

def main():
    jobs = [(square, [x]) for x in range(16)]

    def finished(task, result):
        print task.args, result

    condor.do(jobs, 4, finished)

if __name__ == "__main__":
    condor.enable_default_logging() # XXX

    main()

