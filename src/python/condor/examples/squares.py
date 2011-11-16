import condor

def f(x):
    return x**2

def main():
    def done(task, result):
        print task.args, result

    condor.do([(f, [x]) for x in range(16)], 4, done)

if __name__ == "__main__":
    main()

