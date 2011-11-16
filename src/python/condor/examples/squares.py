import condor

def square(x):
    return x**2

def main():
    jobs = [(square, [x]) for x in range(16)]

    def done(task, result):
        print task.args, result

    condor.do(jobs, 4, done)

if __name__ == "__main__":
    main()

