import condor
import this_module

if __name__ == "__main__":
    this_module.main()

def square(x):
    return x**2

def main():
    jobs = [(square, [x]) for x in range(16)]

    def handle_result(task, xx):
        print task.args, xx

    condor.do_or_distribute(jobs, 4, handle_result)

